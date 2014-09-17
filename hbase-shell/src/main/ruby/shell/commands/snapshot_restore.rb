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
    class SnapshotRestore < Command
      def help
        return <<-EOF
Use snapshot to restore selected tables.
Optional regular expression parameter could be used to filter the output
by table name.

Examples:
  hbase> snapshot_restore
  hbase> snapshot_restore 'abc.*'
EOF
      end

      def command(date)
        now = Time.now.strftime("%Y-%d-%m")
        suffix="-ru-" + date

        list = admin.list_snapshot(".*")
        list.each do |snapshot|
          ssName=snapshot.getName
          if ssName.include? suffix
            formatter.row([ ssName ])
            table=ssName.dup
            begin
              table["_ns_sep_"]=":"
            rescue
            end
            begin
              table[suffix]=""
            rescue
            end

            formatter.row([ "Restoring " + table + " from " + ssName ])
            begin
              admin.disable(table)
              admin.restore_snapshot(ssName)
              admin.enable(table)
            rescue
            end
          end
        end

        return "Done"
      end
    end
  end
end
