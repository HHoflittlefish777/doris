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

#include "vec/exec/format/json/json_parser.h"

namespace doris::vectorized {
Status SimdJsonStreamParser::parse(char* data, size_t len, size_t allocated) noexcept {
    try {
        _json_stream = _ondemand_json_parser->iterate_many(data, len, len);
        _json_stream_iterator = _json_stream.begin();
    } catch (simdjson::simdjson_error& e) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Parse json data for JsonDoc failed. code: {}, error info: {}",
                       e.error(), simdjson::error_message(e.error()));
        return Status::DataQualityError(fmt::to_string(error_msg));
    }
    return Status::OK();
}

Status SimdJsonStreamParser::current_document(
        simdjson::ondemand::document_reference* original_json_doc) noexcept {
    if (_json_stream_iterator != _json_stream.end()) {
        *original_json_doc = (*_json_stream_iterator).value();
        return Status::OK();
    }
    return Status::EndOfFile("read json document end");
}

Status SimdJsonStreamParser::next() noexcept {
    if (++_json_stream_iterator != _json_stream.end()) {
        return Status::OK();
    }
    return Status::EndOfFile("read json document end");
}

Status SimdJsonParser::parse(char* data, size_t len, size_t allocated) noexcept {
    simdjson::error_code error =
            _ondemand_json_parser->iterate(std::string_view(data, len), allocated).get(_doc);
    if (error != simdjson::error_code::SUCCESS) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Parse json data for JsonDoc failed. code: {}, error info: {}",
                       error, simdjson::error_message(error));
        return Status::DataQualityError(fmt::to_string(error_msg));
    }
    return Status::OK();
}

Status SimdJsonParser::current_document(
        simdjson::ondemand::document_reference* original_json_doc) noexcept {
    *original_json_doc = simdjson::ondemand::document_reference(_doc);
    return Status::OK();
}

Status SimdJsonParser::next() noexcept {
    return Status::EndOfFile("read json document end");
}

} // namespace doris::vectorized