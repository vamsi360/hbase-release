#!/bin/sh

#	For sudo make install to work from eclipse; pls do the foll
# In /etc/sudoers change "Defaults    requiretty" to "Defaults:[username] !requiretty"; Here change [username] to the actual user value
#cd ../; make && sudo make install; cd -;

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

echo "LIB PATH = $LD_LIBRARY_PATH"
#DEBUG 
`g++ -g -std=c++11 -o TestClient_d TestClient.cc -lHbaseClient_d -lPocoNet -lPocoFoundation -lglog -lprotobuf -lsasl2`
#RELEASE
`g++ -std=c++11 -o TestClient TestClient.cc -lHbaseClient -lPocoNet -lPocoFoundation -lglog -lprotobuf -lsasl2`


