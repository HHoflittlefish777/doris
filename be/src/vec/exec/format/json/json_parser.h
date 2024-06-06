// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <simdjson.h>
#include <simdjson/simdjson.h>

#include "common/status.h"

namespace doris {
namespace vectorized {

class JsonParser {
public:
    JsonParser(simdjson::ondemand::parser* parser) : _ondemand_json_parser(parser) {};
    virtual ~JsonParser() = default;
    virtual Status parse(char* data, size_t len, size_t allocated) noexcept = 0;
    virtual Status current_document(
            simdjson::ondemand::document_reference original_json_doc) noexcept = 0;
    virtual Status next() noexcept = 0;

protected:
    simdjson::ondemand::parser* _ondemand_json_parser;
};

class SimdJsonStreamParser : public JsonParser {
public:
    SimdJsonStreamParser(simdjson::ondemand::parser* parser) : JsonParser(parser) {};
    Status parse(char* data, size_t len, size_t allocated) noexcept override;
    Status current_document(
            simdjson::ondemand::document_reference original_json_doc) noexcept override;
    Status next() noexcept override;

private:
    simdjson::ondemand::document_stream _json_stream;
    simdjson::ondemand::document_stream::iterator _json_stream_iterator;
};

class SimdJsonParser : public JsonParser {
public:
    SimdJsonParser(simdjson::ondemand::parser* parser) : JsonParser(parser) {};
    Status parse(char* data, size_t len, size_t allocated) noexcept override;
    Status current_document(
            simdjson::ondemand::document_reference original_json_doc) noexcept override;
    Status next() noexcept override;

private:
    simdjson::ondemand::document _doc;
};

} // namespace vectorized
} // namespace doris