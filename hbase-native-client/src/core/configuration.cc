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

#include "configuration.h"

#include <glog/logging.h>
#include <Poco/StringTokenizer.h>

#include "exception.h"

Configuration::Configuration() {

}

Configuration::Configuration(const char *configFile) {

  if (nullptr == configFile)
    throw HBaseException("No configuration file provided.\n");
  std::ifstream in(configFile);
  Poco::XML::InputSource src(in);

  Poco::XML::DOMParser parser;
  Poco::AutoPtr<Poco::XML::Document> pDoc = parser.parse(&src);

  Poco::XML::NodeIterator it(pDoc, Poco::XML::NodeFilter::SHOW_ELEMENT);
  Poco::XML::Node *pNode = it.nextNode();

  while (pNode) {
    std::string nodeName("");
    std::string nodeValue("");
    if ("property" == pNode->nodeName() && pNode->hasChildNodes()) {
      pNode = it.nextNode();
      if ("name" == pNode->nodeName() && pNode->hasChildNodes()) {
        nodeName = pNode->firstChild()->getNodeValue();
        pNode = it.nextNode();
        if ("value" == pNode->nodeName() && pNode->hasChildNodes()) {
          nodeValue = pNode->firstChild()->getNodeValue();
        }
      }
      if (nodeValue.length() > 0) {
        hbaseConfig_.insert(std::pair<std::string, std::string>(nodeName, nodeValue));
      }
    } else
      pNode = it.nextNode();
  }

  ReplaceConfVariables();
}

void Configuration::ReplaceConfVariables() {

  std::vector <std::string> confVarsNotFound;
  for (std::map<std::string, std::string>::iterator itr = hbaseConfig_.begin(); itr != hbaseConfig_.end() ; ++itr) {
    size_t pos = itr->second.find("${");
    if (std::string::npos != pos) {
      size_t posNext = itr->second.find("}", pos+1);
      if (std::string::npos != posNext) {
        std::string actualValue(itr->second.substr(pos+2, posNext-pos-2));
        if(hbaseConfig_[actualValue].length() > 0) {
          hbaseConfig_[itr->first] = itr->second.replace(pos, posNext+1, hbaseConfig_[actualValue]);
        } else {
          confVarsNotFound.push_back(actualValue);
        }
      }
    }
  }
  return;
}
void Configuration::Display() {

  for (std::map<std::string, std::string>::const_iterator itr = hbaseConfig_.begin(); itr != hbaseConfig_.end(); ++itr) {
    DLOG(INFO) << itr->first << ":" << itr->second;
  }

}

const std::string Configuration::GetValue(const std::string &key) {

  std::string config_value("");
  if ("hbase.zookeeper.quorum" == key) {
    Poco::StringTokenizer zk_clients(this->hbaseConfig_[key], ",", Poco::StringTokenizer::TOK_IGNORE_EMPTY);
    std::string zk_quorum_str("");
    for (auto client : zk_clients ) {
      config_value += client;
      config_value += ":";
      config_value += this->hbaseConfig_["hbase.zookeeper.property.clientPort"];
      config_value += ",";
    }
  } else {
    config_value = this->hbaseConfig_[key];
  }
  return config_value;
}


Configuration::~Configuration() {

  hbaseConfig_.clear();

}

