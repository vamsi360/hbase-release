#!/bin/sh
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

#	For sudo make install to work from eclipse; pls do the foll
# In /etc/sudoers change "Defaults    requiretty" to "Defaults:[username] !requiretty"; Here change [username] to the actual user value
#cd ../; make && sudo make install; cd -;

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

echo "LIB PATH = $LD_LIBRARY_PATH"
#DEBUG 
`g++ -g -std=c++11 -o TestClient_d TestClient.cc -lHbaseClient_d -lPocoNet -lPocoFoundation -lglog -lprotobuf -lsasl2`
#RELEASE
`g++ -std=c++11 -o TestClient TestClient.cc -lHbaseClient -lPocoNet -lPocoFoundation -lglog -lprotobuf -lsasl2`


