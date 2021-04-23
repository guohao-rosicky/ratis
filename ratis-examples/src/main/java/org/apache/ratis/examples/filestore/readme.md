

```
start-all-v2.sh filestore server

client.sh filestore loadgen --size 1048576 --numFiles 1 --storage /tmp/ratis/loadgen --peers n0:localhost:6000:6001:6002:6003,n1:localhost:7000:7001:7002:7003,n2:localhost:8000:8001:8002:8003
```







