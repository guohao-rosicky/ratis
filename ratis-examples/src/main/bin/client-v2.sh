#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"




QUORUM_OPTS="--peers n0:localhost:6000:6001:6002:6003,n1:localhost:7000:7001:7002:7003,n2:localhost:8000:8001:8002:8003"

cd $DIR

./client.sh filestore loadgen --size 1048576 --numFiles 1 --storage /Users/rosicky/test/ratis/loadgen/ $QUORUM_OPTS

