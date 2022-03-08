/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.netty.client;

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestFilePositionCount;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.NettyDataStreamUtils;
import org.apache.ratis.netty.NettyUtils;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.bootstrap.Bootstrap;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.Channel;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelFutureListener;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandler;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.socket.SocketChannel;
import org.apache.ratis.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class NettyClientStreamRpc implements DataStreamClientRpc {
  public static final Logger LOG = LoggerFactory.getLogger(NettyClientStreamRpc.class);

  private static class WorkerGroupGetter implements Supplier<EventLoopGroup> {
    private static final AtomicReference<EventLoopGroup> SHARED_WORKER_GROUP = new AtomicReference<>();

    static EventLoopGroup newWorkerGroup(RaftProperties properties) {
      return NettyUtils.newEventLoopGroup("StreamClient-NioEventLoopGroup",
          NettyConfigKeys.DataStream.clientWorkerGroupSize(properties), false);
    }

    private final EventLoopGroup workerGroup;
    private final boolean ignoreShutdown;

    WorkerGroupGetter(RaftProperties properties) {
      if (NettyConfigKeys.DataStream.clientWorkerGroupShare(properties)) {
        workerGroup = SHARED_WORKER_GROUP.updateAndGet(g -> g != null? g: newWorkerGroup(properties));
        ignoreShutdown = true;
      } else {
        workerGroup = newWorkerGroup(properties);
        ignoreShutdown = false;
      }
    }

    @Override
    public EventLoopGroup get() {
      return workerGroup;
    }

    void shutdownGracefully() {
      if (!ignoreShutdown) {
        workerGroup.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS);
        try {
          workerGroup.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOG.error("group shutdown error:", e);
        }
      }
    }
  }

  static class ReplyQueue implements Iterable<CompletableFuture<DataStreamReply>> {
    static final ReplyQueue EMPTY = new ReplyQueue();

    private final Queue<CompletableFuture<DataStreamReply>> queue = new ConcurrentLinkedQueue<>();
    private int emptyId;

    /** @return an empty ID if the queue is empty; otherwise, the queue is non-empty, return null. */
    synchronized Integer getEmptyId() {
      return queue.isEmpty()? emptyId: null;
    }

    synchronized boolean offer(CompletableFuture<DataStreamReply> f) {
      if (queue.offer(f)) {
        emptyId++;
        return true;
      }
      return false;
    }

    CompletableFuture<DataStreamReply> poll() {
      return queue.poll();
    }

    int size() {
      return queue.size();
    }

    @Override
    public Iterator<CompletableFuture<DataStreamReply>> iterator() {
      return queue.iterator();
    }
  }

  private final String name;
  private final WorkerGroupGetter workerGroup;
  //private final Supplier<Channel> channel;
  private final AtomicReference<Channel> channel = new AtomicReference<>();

  private final ConcurrentMap<ClientInvocationId, ReplyQueue> replies = new ConcurrentHashMap<>();
  private final TimeDuration replyQueueGracePeriod;
  private final TimeoutScheduler timeoutScheduler = TimeoutScheduler.getInstance();

  private final String dataStreamAddress;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public NettyClientStreamRpc(RaftPeer server, RaftProperties properties){
    this.name = JavaUtils.getClassSimpleName(getClass()) + "->" + server;
    this.workerGroup = new WorkerGroupGetter(properties);
    this.dataStreamAddress = server.getDataStreamAddress();
    this.replyQueueGracePeriod = NettyConfigKeys.DataStream.clientReplyQueueGracePeriod(properties);
    connect();
  }

  private void connect() {
    ChannelFuture f = new Bootstrap()
        .group(getWorkerGroup())
        .channel(NioSocketChannel.class)
        .handler(getInitializer())
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .connect(NetUtils.createSocketAddr(dataStreamAddress))
        .addListener(new ConnectionListener(this));
    Channel channel = f.syncUninterruptibly().channel();
    this.channel.getAndSet(channel);
  }

  private static class ConnectionListener implements ChannelFutureListener {
    private final NettyClientStreamRpc client;

    public ConnectionListener(NettyClientStreamRpc client) {
      this.client = client;
    }

    @Override
    public void operationComplete(ChannelFuture channelFuture) throws Exception {
      if (!channelFuture.isSuccess()) {
        LOG.warn("connect server {} failed, reconnect.", this.client.getDataStreamAddress(), channelFuture.cause());

        client.getWorkerGroup().schedule(
            client::connect, 100, TimeUnit.MILLISECONDS
        );
      } else {
        LOG.info("connect successful.");
      }
    }
  }

  public String getDataStreamAddress() {
    return dataStreamAddress;
  }

  public EventLoopGroup getWorkerGroup() {
    return workerGroup.get();
  }

  private Channel getChannel() {
    if (!isClosed.get() && channel.get().isOpen()) {
      return channel.get();
    } else if (isClosed.get()) {
      LOG.warn("client is not writable.");
      return null;
    } else {
      connect();
      return channel.get();
    }
    //closeInternal();
  }

  private ChannelInboundHandler getClientHandler(){
    return new ChannelInboundHandlerAdapter(){


      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {

        ClientInvocationId cid = null;

        try {
          if (!(msg instanceof DataStreamReply)) {
            LOG.error("{}: unexpected message {}", this, msg.getClass());
            return;
          }
          final DataStreamReply reply = (DataStreamReply) msg;
          LOG.debug("{}: read {}", this, reply);
          final ClientInvocationId clientInvocationId = ClientInvocationId.valueOf(reply.getClientId(), reply.getStreamId());
          cid = clientInvocationId;
          final ReplyQueue queue = reply.isSuccess() ? replies.get(clientInvocationId) :
                  replies.remove(clientInvocationId);
          if (queue != null) {
            final CompletableFuture<DataStreamReply> f = queue.poll();
            if (f != null) {
              f.complete(reply);

              if (!reply.isSuccess() && queue.size() > 0) {
                final IllegalStateException e = new IllegalStateException(
                    this + ": an earlier request failed with " + reply);
                queue.forEach(future -> future.completeExceptionally(e));
              }

              final Integer emptyId = queue.getEmptyId();
              if (emptyId != null) {
                timeoutScheduler.onTimeout(replyQueueGracePeriod,
                    // remove the queue if the same queue has been empty for the entire grace period.
                    () -> replies.computeIfPresent(clientInvocationId,
                        (key, q) -> q == queue && emptyId.equals(q.getEmptyId())? null: q),
                    LOG, () -> "Timeout check failed, clientInvocationId=" + clientInvocationId);
              }
            }
          }

        } catch (Throwable e) {
          LOG.error("channel read error:", e);

          Optional.ofNullable(cid)
              .map(replies::remove)
              .orElse(ReplyQueue.EMPTY)
              .forEach(f -> f.completeExceptionally(e));

          ctx.close();
        }
      }

      @Override
      public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("connected server {}", ctx.channel().remoteAddress());
      }

      @Override
      public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!isClosed.get()) {
          LOG.warn("client is inactive, reconnect to {}", ctx.channel().remoteAddress());
          getWorkerGroup().schedule(
              () -> connect(), 100, TimeUnit.MILLISECONDS
          );
        }
        super.channelInactive(ctx);
      }




      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.warn(name + ": exceptionCaught", cause);
        ctx.close();
      }
    };
  }

  private ChannelInitializer<SocketChannel> getInitializer(){
    return new ChannelInitializer<SocketChannel>(){
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(newEncoder());
        p.addLast(newEncoderDataStreamRequestFilePositionCount());
        p.addLast(newDecoder());
        //p.addLast(new ReadTimeoutHandler(30));
        p.addLast(getClientHandler());
      }
    };
  }

  MessageToMessageEncoder<DataStreamRequestByteBuffer> newEncoder() {
    return new MessageToMessageEncoder<DataStreamRequestByteBuffer>() {
      @Override
      protected void encode(ChannelHandlerContext context, DataStreamRequestByteBuffer request, List<Object> out) {
        NettyDataStreamUtils.encodeDataStreamRequestByteBuffer(request, out::add, context.alloc());
      }
    };
  }

  MessageToMessageEncoder<DataStreamRequestFilePositionCount> newEncoderDataStreamRequestFilePositionCount() {
    return new MessageToMessageEncoder<DataStreamRequestFilePositionCount>() {
      @Override
      protected void encode(ChannelHandlerContext ctx, DataStreamRequestFilePositionCount request, List<Object> out) {
        NettyDataStreamUtils.encodeDataStreamRequestFilePositionCount(request, out::add, ctx.alloc());
      }
    };
  }

  ByteToMessageDecoder newDecoder() {
    return new ByteToMessageDecoder() {
      {
        this.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
      }

      @Override
      protected void decode(ChannelHandlerContext context, ByteBuf buf, List<Object> out) {
        Optional.ofNullable(NettyDataStreamUtils.decodeDataStreamReplyByteBuffer(buf)).ifPresent(out::add);
      }
    };
  }

  @Override
  public CompletableFuture<DataStreamReply> streamAsync(DataStreamRequest request) {
    final CompletableFuture<DataStreamReply> f = new CompletableFuture<>();
    ClientInvocationId clientInvocationId = ClientInvocationId.valueOf(request.getClientId(), request.getStreamId());
    final ReplyQueue q = replies.computeIfAbsent(clientInvocationId, key -> new ReplyQueue());
    if (!q.offer(f)) {
      f.completeExceptionally(new IllegalStateException(this + ": Failed to offer a future for " + request));
      return f;
    }
    LOG.debug("{}: write {}", this, request);
    Channel channel = getChannel();
    if (channel != null) {
      channel.writeAndFlush(request).addListener(future -> {
        if (!future.isSuccess()) {
          Throwable cause = future.cause();
          LOG.error("Netty client send request error:", cause);
          if (!f.isDone()) {
            f.completeExceptionally(new IllegalStateException(this + "writable Failed to send request for " + request, cause));
          }
        }
      });
    } else {
      f.completeExceptionally(new IllegalStateException(this + ": Netty channel is closed Failed to send request for " + request));
    }
    return f;
  }

  @Override
  public void close() {
    LOG.info("close stream client " + name);
    isClosed.getAndSet(true);
    closeInternal();
    workerGroup.shutdownGracefully();
    LOG.info("close stream client done" + name);
  }

  private void closeInternal() {
    Channel c = this.channel.get();
    if (c != null) {
      c.close().syncUninterruptibly();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
