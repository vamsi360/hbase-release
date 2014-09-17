#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'time'

module Shell
  module Commands
    class SnapshotAll < Command
      def help
        return <<-EOF
List all tables and take snapshot .
Optional regular expression parameter could be used to filter the output
by table name.

Examples:
  hbase> snapshot_all
  hbase> snapshot_all 'abc.*'
EOF
      end

      def command(regex = ".*")
        now = Time.now.strftime("%Y%m%d")

        list = admin.list(regex)
        list.each do |table|
          ssName=table.dup
          begin
            ssName[":"]="_ns_sep_"
          rescue
          end

          ssName=ssName + "-ru-" + now
          begin
            admin.delete_snapshot(ssName)
          rescue
          end
          begin
            admin.snapshot(table, ssName)
          rescue
          end
          formatter.row([ table ])
        end

        return list
      end
    end
  end
end
