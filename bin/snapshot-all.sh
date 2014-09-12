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
DATE=`date +"%Y%m%d"`

filename="/tmp/table-list-$DATE"
echo "list" | hbase shell > $filename
# whether the current line represents a table
isTable=0
#number of tables snapshotted
tablesSnapshotted=0

while read line
do
  if [[ "$line" == "TABLE" ]];
  then
    isTable=1
    continue
  fi
  if [[ "$line" =~ "row(s)" ]];
  then
    exit
  fi
  if [[ $isTable == 1 ]];
  then
    tblName=`echo $line | sed s"/\:/_ns_sep_/"`
    echo "delete_snapshot '$tblName-ru-$DATE'" | hbase shell
    echo "snapshot '$line', '$tblName-ru-$DATE'" | hbase shell
    tablesSnapshotted=$[$tablesSnapshotted+1]
  fi
  echo "$tablesSnapshotted tables have been snapshotted"
  
done < "$filename"
