#!/bin/bash

#/**
#
#* Licensed under the Apache License, Version 2.0 (the "License");
#* you may not use this file except in compliance with the License.
#* You may obtain a copy of the License at
#
#* http://www.apache.org/licenses/LICENSE-2.0
#
#* Unless required by applicable law or agreed to in writing, software
#* distributed under the License is distributed on an "AS IS" BASIS,
#* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#* See the License for the specific language governing permissions and
#* limitations under the License.
#
#*/
DATE=$1
if [ "x${DATE}x" == "xx" ]; then
  echo "Must provide date when the snapshot-all.sh script was run"
  exit 1
fi

filename="/tmp/snapshot-list-$DATE"
echo "list_snapshots" | hbase shell > $filename
# whether the current line represents a snapshot
isSnapshot=0
#number of snapshots restored
snapshotsRestored=0
suffix="-ru-$DATE"

while read line
do
  if [[ "$line" =~ "SNAPSHOT" ]];
  then
    isSnapshot=1
    continue
  fi
  if [[ "$line" =~ "row(s)" ]];
  then
    exit
  fi
  if [[ $isSnapshot == 1 ]];
  then
    arr=$(echo $line | tr " " "\n")

    for snapshotName in $arr
    do
      if [[ "$snapshotName" =~ $suffix ]]; then
        tableName=`echo $snapshotName | sed s"/_ns_sep_/\:/" | sed "s/-ru-$DATE$//"`
        echo "disable '$tableName'" | hbase shell
        echo "restore_snapshot '$snapshotName'" | hbase shell
        echo "enable '$tableName'" | hbase shell
        snapshotsRestored=$[$snapshotsRestored+1]
      fi
    done
  fi
  echo "$snapshotsRestored tables have been restored"
  
done < "$filename"
