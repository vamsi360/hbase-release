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
 *
 */

#include "family_filter.h"

const char *FamilyFilter::FILTER_NAME = "org.apache.hadoop.hbase.filter.FamilyFilter";

FamilyFilter::FamilyFilter(const CompareOp &compare_op,
    const ByteArrayComparable *comparator):
                                                       CompareFilter(compare_op, comparator){

}

FamilyFilter::FamilyFilter() {
  // TODO Auto-generated destructor stub
}

FamilyFilter::~FamilyFilter() {
  // TODO Auto-generated destructor stub
}

Filter::ReturnCode FamilyFilter::FilterKeyValue(const Cell &cell) {
  int family_length = 0;
  if (family_length > 0) {

  }
  return Filter::ReturnCode::INCLUDE;
}

const char *FamilyFilter::GetName() {

  return FamilyFilter::FILTER_NAME;
}

bool FamilyFilter::ToByteArray(hbase::pb::Filter &filter) {

  bool status = false;
  hbase::pb::CompareFilter comp_filter;
  google::protobuf::Message *msg = &comp_filter;
  std::unique_ptr<google::protobuf::Message> comparefilter_msg(this->Convert());
  msg = comparefilter_msg.get();
  if (nullptr != msg) {
    hbase::pb::CompareFilter *comp_filter_resp = dynamic_cast<hbase::pb::CompareFilter *>(msg);
    hbase::pb::FamilyFilter *family_filter = new hbase::pb::FamilyFilter();
    family_filter->set_allocated_compare_filter(comp_filter_resp);
    filter.set_name(this->GetName());
    filter.set_serialized_filter(family_filter->SerializeAsString());
    status = true;
  }
  return status;
}
