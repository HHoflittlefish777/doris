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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <string_view>

#include "util/mysql_row_buffer.h"
#include "vec/core/field.h"
#include "vec/core/types.h" // UInt32
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/io/reader_buffer.h"

using namespace doris;
using namespace doris::vectorized;

static void from_string_checker(UInt32 scale, const std::string& rounding,
                                const std::string& expected) {
    // DataTypeDateTimeV2
    std::shared_ptr<const DataTypeDateTimeV2> datetime_ptr =
            std::dynamic_pointer_cast<const DataTypeDateTimeV2>(
                    DataTypeFactory::instance().create_data_type(
                            doris::FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 0, scale));

    // constructor of ReadBuffer is not const(which seems not reasonable),
    // so we need to cast away const
    ReadBuffer rb(const_cast<char*>(rounding.c_str()), rounding.size());
    ColumnUInt64::MutablePtr column = ColumnUInt64::create(0);
    // DataTypeDateTimeV2::from_string
    auto rt = datetime_ptr->from_string(rb, &(*column));
    EXPECT_TRUE(rt.ok());
    EXPECT_EQ(rt.msg(), ""); // so that we can print error msg if failed
    EXPECT_EQ(datetime_ptr->to_string(*column, 0), expected);
};

static void from_thrift_checker(UInt32 scale, const String& input, const String& expected) {
    TScalarType tscale_type;
    tscale_type.type = TPrimitiveType::DATETIMEV2;
    tscale_type.precision = 18;
    tscale_type.scale = scale;

    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    type_node.scalar_type = tscale_type;

    TTypeDesc type_desc;
    type_desc.types.push_back(type_node);

    TDateLiteral date_literal;
    date_literal.value = input;

    TExprNode expr_node;
    expr_node.node_type = TExprNodeType::DATE_LITERAL;
    expr_node.type = type_desc;
    expr_node.date_literal = date_literal;

    std::shared_ptr<const DataTypeDateTimeV2> datetime_ptr =
            std::dynamic_pointer_cast<const DataTypeDateTimeV2>(
                    DataTypeFactory::instance().create_data_type(
                            doris::FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 0, scale));

    auto field = datetime_ptr->get_field(expr_node);
    uint64_t value = 0;
    //  = datetime_ptr->get_storage_field_type();
    EXPECT_EQ(field.try_get(value), true);
    auto column = datetime_ptr->create_column_const(1, field);
    EXPECT_EQ(datetime_ptr->to_string(*column, 1), expected);
}

static void serialization_checker(UInt32 scale, const std::string& input,
                                  const std::string& expected) {
    std::shared_ptr<const DataTypeDateTimeV2> datetime_ptr =
            std::dynamic_pointer_cast<const DataTypeDateTimeV2>(
                    DataTypeFactory::instance().create_data_type(
                            doris::FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 0, scale));

    ReadBuffer rb(const_cast<char*>(input.c_str()), input.size());
    ColumnUInt64::MutablePtr column = ColumnUInt64::create(0);
    auto rt = datetime_ptr->from_string(rb, &(*column));
    EXPECT_TRUE(rt.ok());
    auto serde = std::dynamic_pointer_cast<DataTypeDateTimeV2SerDe>(datetime_ptr->get_serde());
    MysqlRowBuffer<false> mysql_rb;
    rt = serde->write_column_to_mysql(*column, mysql_rb, 0, false);
    EXPECT_TRUE(rt.ok());
    auto elem_size = static_cast<uint8_t>(*mysql_rb.buf());
    if (elem_size != expected.size()) {
        std::cerr << "Left size " << elem_size << " right size " << expected.size() << " left str "
                  << std::string_view(mysql_rb.buf() + 1, elem_size) << " right str " << expected
                  << std::endl;
        ASSERT_TRUE(false); // terminate ut
    }
    EXPECT_EQ(std::string_view(mysql_rb.buf() + 1, expected.size()), expected);
}

static std::multimap<size_t /*scale*/,
                     std::multimap<std::string /*input*/, std::string /*expected*/>>
        scale_with_input_and_expected = {
                {0, {{"2020-01-01 12:00:00", "2020-01-01 12:00:00"}}},
                {0, {{"2020-01-01 12:00:00.0", "2020-01-01 12:00:00"}}},
                {0, {{"2020-01-01 12:00:00.5", "2020-01-01 12:00:01"}}},
                {0, {{"2020-01-01 12:00:00.49", "2020-01-01 12:00:00"}}},
                {0, {{"2020-01-01 12:00:00.00", "2020-01-01 12:00:00"}}},
                {0, {{"2020-01-01 12:00:00.999999999999", "2020-01-01 12:00:01"}}},
                {0, {{"9999-12-31 23:59:59", "9999-12-31 23:59:59"}}},
                // normal cast, no rounding
                {1, {{"2020-01-01 12:00:00", "2020-01-01 12:00:00.0"}}},
                {1, {{"2020-01-01 12:00:00.0", "2020-01-01 12:00:00.0"}}},
                {1, {{"2020-01-01 12:00:00.1", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.2", "2020-01-01 12:00:00.2"}}},
                {1, {{"2020-01-01 12:00:00.3", "2020-01-01 12:00:00.3"}}},
                {1, {{"2020-01-01 12:00:00.4", "2020-01-01 12:00:00.4"}}},
                {1, {{"2020-01-01 12:00:00.5", "2020-01-01 12:00:00.5"}}},
                {1, {{"2020-01-01 12:00:00.6", "2020-01-01 12:00:00.6"}}},
                {1, {{"2020-01-01 12:00:00.7", "2020-01-01 12:00:00.7"}}},
                {1, {{"2020-01-01 12:00:00.8", "2020-01-01 12:00:00.8"}}},
                {1, {{"2020-01-01 12:00:00.9", "2020-01-01 12:00:00.9"}}},
                // round test
                {1, {{"2020-01-01 12:00:00.10", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.11", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.12", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.13", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.14", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.15", "2020-01-01 12:00:00.2"}}},
                {1, {{"2020-01-01 12:00:00.16", "2020-01-01 12:00:00.2"}}},
                {1, {{"2020-01-01 12:00:00.17", "2020-01-01 12:00:00.2"}}},
                {1, {{"2020-01-01 12:00:00.18", "2020-01-01 12:00:00.2"}}},
                {1, {{"2020-01-01 12:00:00.19", "2020-01-01 12:00:00.2"}}},
                {1, {{"2020-01-01 12:00:00.90", "2020-01-01 12:00:00.9"}}},
                {1, {{"2020-01-01 12:00:00.91", "2020-01-01 12:00:00.9"}}},
                {1, {{"2020-01-01 12:00:00.92", "2020-01-01 12:00:00.9"}}},
                {1, {{"2020-01-01 12:00:00.93", "2020-01-01 12:00:00.9"}}},
                {1, {{"2020-01-01 12:00:00.94", "2020-01-01 12:00:00.9"}}},
                {1, {{"2020-01-01 12:00:00.95", "2020-01-01 12:00:01.0"}}},
                {1, {{"2020-01-01 12:00:00.96", "2020-01-01 12:00:01.0"}}},
                {1, {{"2020-01-01 12:00:00.97", "2020-01-01 12:00:01.0"}}},
                {1, {{"2020-01-01 12:00:00.98", "2020-01-01 12:00:01.0"}}},
                {1, {{"2020-01-01 12:00:00.99", "2020-01-01 12:00:01.0"}}},
                // make sure we are doing truncate instead of round up from last digit
                {1, {{"2020-01-01 12:00:00.140", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.141", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.142", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.143", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.144", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.145", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.146", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.147", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.148", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.149", "2020-01-01 12:00:00.1"}}},
                {1, {{"2020-01-01 12:00:00.150", "2020-01-01 12:00:00.2"}}},
                {1, {{"9999-12-31 23:59:59.9", "9999-12-31 23:59:59.9"}}},
                // normal cast, no rounding
                {2, {{"2020-01-01 12:00:00", "2020-01-01 12:00:00.00"}}},
                {2, {{"2020-01-01 12:00:00.1", "2020-01-01 12:00:00.10"}}},
                {2, {{"2020-01-01 12:00:00.00", "2020-01-01 12:00:00.00"}}},
                {2, {{"2020-01-01 12:00:00.01", "2020-01-01 12:00:00.01"}}},
                {2, {{"2020-01-01 12:00:00.02", "2020-01-01 12:00:00.02"}}},
                {2, {{"2020-01-01 12:00:00.03", "2020-01-01 12:00:00.03"}}},
                {2, {{"2020-01-01 12:00:00.04", "2020-01-01 12:00:00.04"}}},
                {2, {{"2020-01-01 12:00:00.05", "2020-01-01 12:00:00.05"}}},
                {2, {{"2020-01-01 12:00:00.06", "2020-01-01 12:00:00.06"}}},
                {2, {{"2020-01-01 12:00:00.07", "2020-01-01 12:00:00.07"}}},
                {2, {{"2020-01-01 12:00:00.08", "2020-01-01 12:00:00.08"}}},
                {2, {{"2020-01-01 12:00:00.09", "2020-01-01 12:00:00.09"}}},
                {2, {{"2020-01-01 12:00:00.12", "2020-01-01 12:00:00.12"}}},
                // rounding
                {2, {{"2020-01-01 12:00:00.990", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.991", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.992", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.993", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.994", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.995", "2020-01-01 12:00:01.00"}}},
                {2, {{"2020-01-01 12:00:00.996", "2020-01-01 12:00:01.00"}}},
                {2, {{"2020-01-01 12:00:00.997", "2020-01-01 12:00:01.00"}}},
                {2, {{"2020-01-01 12:00:00.998", "2020-01-01 12:00:01.00"}}},
                {2, {{"2020-01-01 12:00:00.999", "2020-01-01 12:00:01.00"}}},
                // make sure we are doing truncate instead of round from last digit
                {2, {{"2020-01-01 12:00:00.9940", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9941", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9942", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9943", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9944", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9945", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9946", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9947", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9948", "2020-01-01 12:00:00.99"}}},
                {2, {{"2020-01-01 12:00:00.9949", "2020-01-01 12:00:00.99"}}},
                {2, {{"9999-12-31 23:59:59.99", "9999-12-31 23:59:59.99"}}},
                {3, {{"2020-01-01 12:00:00", "2020-01-01 12:00:00.000"}}},
                {3, {{"2020-01-01 12:00:00.1", "2020-01-01 12:00:00.100"}}},
                {3, {{"2020-01-01 12:00:00.00", "2020-01-01 12:00:00.000"}}},
                {3, {{"2020-01-01 12:00:00.01", "2020-01-01 12:00:00.010"}}},
                {3, {{"2020-01-01 12:00:00.001", "2020-01-01 12:00:00.001"}}},
                {3, {{"2020-01-01 12:00:00.002", "2020-01-01 12:00:00.002"}}},
                {3, {{"2020-01-01 12:00:00.003", "2020-01-01 12:00:00.003"}}},
                {3, {{"2020-01-01 12:00:00.004", "2020-01-01 12:00:00.004"}}},
                {3, {{"2020-01-01 12:00:00.005", "2020-01-01 12:00:00.005"}}},
                {3, {{"2020-01-01 12:00:00.006", "2020-01-01 12:00:00.006"}}},
                {3, {{"2020-01-01 12:00:00.007", "2020-01-01 12:00:00.007"}}},
                {3, {{"2020-01-01 12:00:00.008", "2020-01-01 12:00:00.008"}}},
                {3, {{"2020-01-01 12:00:00.009", "2020-01-01 12:00:00.009"}}},
                {3, {{"2020-01-01 12:00:00.123", "2020-01-01 12:00:00.123"}}},
                {3, {{"2020-01-01 12:00:00.999", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.123", "2020-01-01 12:00:00.123"}}},
                // rounding
                {3, {{"2020-01-01 12:00:00.9990", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.9991", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.9992", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.9993", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.9994", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.9995", "2020-01-01 12:00:01.000"}}},
                {3, {{"2020-01-01 12:00:00.9996", "2020-01-01 12:00:01.000"}}},
                {3, {{"2020-01-01 12:00:00.9997", "2020-01-01 12:00:01.000"}}},
                {3, {{"2020-01-01 12:00:00.9998", "2020-01-01 12:00:01.000"}}},
                {3, {{"2020-01-01 12:00:00.9999", "2020-01-01 12:00:01.000"}}},
                // make sure we are doing truncate instead of round from last digit
                {3, {{"2020-01-01 12:00:00.99940", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99941", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99942", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99943", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99944", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99945", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99946", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99947", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99948", "2020-01-01 12:00:00.999"}}},
                {3, {{"2020-01-01 12:00:00.99949", "2020-01-01 12:00:00.999"}}},
                {3, {{"9999-12-31 23:59:59.999", "9999-12-31 23:59:59.999"}}},
                // normal cast, no rounding
                {4, {{"2020-01-01 12:00:00", "2020-01-01 12:00:00.0000"}}},
                {4, {{"2020-01-01 12:00:00.1", "2020-01-01 12:00:00.1000"}}},
                {4, {{"2020-01-01 12:00:00.01", "2020-01-01 12:00:00.0100"}}},
                {4, {{"2020-01-01 12:00:00.001", "2020-01-01 12:00:00.0010"}}},
                {4, {{"2020-01-01 12:00:00.0001", "2020-01-01 12:00:00.0001"}}},
                {4, {{"2020-01-01 12:00:00.0002", "2020-01-01 12:00:00.0002"}}},
                {4, {{"2020-01-01 12:00:00.0003", "2020-01-01 12:00:00.0003"}}},
                {4, {{"2020-01-01 12:00:00.0004", "2020-01-01 12:00:00.0004"}}},
                {4, {{"2020-01-01 12:00:00.0005", "2020-01-01 12:00:00.0005"}}},
                {4, {{"2020-01-01 12:00:00.0006", "2020-01-01 12:00:00.0006"}}},
                {4, {{"2020-01-01 12:00:00.0007", "2020-01-01 12:00:00.0007"}}},
                {4, {{"2020-01-01 12:00:00.0008", "2020-01-01 12:00:00.0008"}}},
                {4, {{"2020-01-01 12:00:00.0009", "2020-01-01 12:00:00.0009"}}},
                {4, {{"2020-01-01 12:00:00.1234", "2020-01-01 12:00:00.1234"}}},
                {4, {{"2020-01-01 12:00:00.9999", "2020-01-01 12:00:00.9999"}}},
                // rounding
                {4, {{"2020-01-01 12:00:00.99990", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.99991", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.99992", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.99993", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.99994", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.99995", "2020-01-01 12:00:01.0000"}}},
                {4, {{"2020-01-01 12:00:00.99996", "2020-01-01 12:00:01.0000"}}},
                {4, {{"2020-01-01 12:00:00.99997", "2020-01-01 12:00:01.0000"}}},
                {4, {{"2020-01-01 12:00:00.99998", "2020-01-01 12:00:01.0000"}}},
                {4, {{"2020-01-01 12:00:00.99999", "2020-01-01 12:00:01.0000"}}},
                // make sure we are doing truncate instead of round from last digit
                {4, {{"2020-01-01 12:00:00.999940", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999941", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999942", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999943", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999944", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999945", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999946", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999947", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999948", "2020-01-01 12:00:00.9999"}}},
                {4, {{"2020-01-01 12:00:00.999949", "2020-01-01 12:00:00.9999"}}},
                {4, {{"9999-12-31 23:59:59.9999", "9999-12-31 23:59:59.9999"}}},
                {5, {{"2020-01-01 12:00:00", "2020-01-01 12:00:00.00000"}}},
                {5, {{"2020-01-01 12:00:00.1", "2020-01-01 12:00:00.10000"}}},
                {5, {{"2020-01-01 12:00:00.01", "2020-01-01 12:00:00.01000"}}},
                {5, {{"2020-01-01 12:00:00.001", "2020-01-01 12:00:00.00100"}}},
                {5, {{"2020-01-01 12:00:00.0001", "2020-01-01 12:00:00.00010"}}},
                {5, {{"2020-01-01 12:00:00.00001", "2020-01-01 12:00:00.00001"}}},
                {5, {{"2020-01-01 12:00:00.00002", "2020-01-01 12:00:00.00002"}}},
                {5, {{"2020-01-01 12:00:00.00003", "2020-01-01 12:00:00.00003"}}},
                {5, {{"2020-01-01 12:00:00.00004", "2020-01-01 12:00:00.00004"}}},
                {5, {{"2020-01-01 12:00:00.00005", "2020-01-01 12:00:00.00005"}}},
                {5, {{"2020-01-01 12:00:00.00006", "2020-01-01 12:00:00.00006"}}},
                {5, {{"2020-01-01 12:00:00.00007", "2020-01-01 12:00:00.00007"}}},
                {5, {{"2020-01-01 12:00:00.00008", "2020-01-01 12:00:00.00008"}}},
                {5, {{"2020-01-01 12:00:00.00009", "2020-01-01 12:00:00.00009"}}},
                {5, {{"2020-01-01 12:00:00.12345", "2020-01-01 12:00:00.12345"}}},
                {5, {{"2020-01-01 12:00:00.99999", "2020-01-01 12:00:00.99999"}}},
                // rounding
                {5, {{"2020-01-01 12:00:00.999990", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.999991", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.999992", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.999993", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.999994", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.999995", "2020-01-01 12:00:01.00000"}}},
                {5, {{"2020-01-01 12:00:00.999996", "2020-01-01 12:00:01.00000"}}},
                {5, {{"2020-01-01 12:00:00.999997", "2020-01-01 12:00:01.00000"}}},
                {5, {{"2020-01-01 12:00:00.999998", "2020-01-01 12:00:01.00000"}}},
                {5, {{"2020-01-01 12:00:00.999999", "2020-01-01 12:00:01.00000"}}},
                // make sure we are doing truncate instead of round from last digit
                {5, {{"2020-01-01 12:00:00.9999940", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999941", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999942", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999943", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999944", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999945", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999946", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999947", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999948", "2020-01-01 12:00:00.99999"}}},
                {5, {{"2020-01-01 12:00:00.9999949", "2020-01-01 12:00:00.99999"}}},
                {5, {{"9999-12-31 23:59:59.99999", "9999-12-31 23:59:59.99999"}}},
                // normal cast, no rounding
                {6, {{"2020-01-01 12:00:00", "2020-01-01 12:00:00.000000"}}},
                {6, {{"2020-01-01 12:00:00.1", "2020-01-01 12:00:00.100000"}}},
                {6, {{"2020-01-01 12:00:00.01", "2020-01-01 12:00:00.010000"}}},
                {6, {{"2020-01-01 12:00:00.001", "2020-01-01 12:00:00.001000"}}},
                {6, {{"2020-01-01 12:00:00.0001", "2020-01-01 12:00:00.000100"}}},
                {6, {{"2020-01-01 12:00:00.00001", "2020-01-01 12:00:00.000010"}}},
                {6, {{"2020-01-01 12:00:00.000001", "2020-01-01 12:00:00.000001"}}},
                {6, {{"2020-01-01 12:00:00.000002", "2020-01-01 12:00:00.000002"}}},
                {6, {{"2020-01-01 12:00:00.000003", "2020-01-01 12:00:00.000003"}}},
                {6, {{"2020-01-01 12:00:00.000004", "2020-01-01 12:00:00.000004"}}},
                {6, {{"2020-01-01 12:00:00.000005", "2020-01-01 12:00:00.000005"}}},
                {6, {{"2020-01-01 12:00:00.000006", "2020-01-01 12:00:00.000006"}}},
                {6, {{"2020-01-01 12:00:00.000007", "2020-01-01 12:00:00.000007"}}},
                {6, {{"2020-01-01 12:00:00.000008", "2020-01-01 12:00:00.000008"}}},
                {6, {{"2020-01-01 12:00:00.000009", "2020-01-01 12:00:00.000009"}}},
                {6, {{"2020-01-01 12:00:00.123456", "2020-01-01 12:00:00.123456"}}},
                {6, {{"2020-01-01 12:00:00.999999", "2020-01-01 12:00:00.999999"}}},
                // rounding
                {6, {{"2020-01-01 12:00:00.9999990", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.9999991", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.9999992", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.9999993", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.9999994", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.9999995", "2020-01-01 12:00:01.000000"}}},
                {6, {{"2020-01-01 12:00:00.9999996", "2020-01-01 12:00:01.000000"}}},
                {6, {{"2020-01-01 12:00:00.9999997", "2020-01-01 12:00:01.000000"}}},
                {6, {{"2020-01-01 12:00:00.9999998", "2020-01-01 12:00:01.000000"}}},
                {6, {{"2020-01-01 12:00:00.9999999", "2020-01-01 12:00:01.000000"}}},
                // make sure we are doing truncate instead of round from last digit
                {6, {{"2020-01-01 12:00:00.99999940", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999941", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999942", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999943", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999944", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999945", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999946", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999947", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999948", "2020-01-01 12:00:00.999999"}}},
                {6, {{"2020-01-01 12:00:00.99999949", "2020-01-01 12:00:00.999999"}}},
                {6, {{"9999-12-31 23:59:59.999999", "9999-12-31 23:59:59.999999"}}},
                //
                {0, {{"2024-02-29 23:59:59.9", "2024-03-01 00:00:00"}}},
                {1, {{"2024-02-29 23:59:59.99", "2024-03-01 00:00:00.0"}}},
                {2, {{"2024-02-29 23:59:59.999", "2024-03-01 00:00:00.00"}}},
                {3, {{"2024-02-29 23:59:59.9999", "2024-03-01 00:00:00.000"}}},
                {4, {{"2024-02-29 23:59:59.99999", "2024-03-01 00:00:00.0000"}}},
                {5, {{"2024-02-29 23:59:59.999999", "2024-03-01 00:00:00.00000"}}},
                {6, {{"2024-02-29 23:59:59.9999999", "2024-03-01 00:00:00.000000"}}},
                //
                {0, {{"2025-02-28 23:59:59.9999999", "2025-03-01 00:00:00"}}},
                {1, {{"2025-02-28 23:59:59.9999999", "2025-03-01 00:00:00.0"}}},
                {2, {{"2025-02-28 23:59:59.9999999", "2025-03-01 00:00:00.00"}}},
                {3, {{"2025-02-28 23:59:59.9999999", "2025-03-01 00:00:00.000"}}},
                {4, {{"2025-02-28 23:59:59.9999999", "2025-03-01 00:00:00.0000"}}},
                {5, {{"2025-02-28 23:59:59.9999999", "2025-03-01 00:00:00.00000"}}},
                {6, {{"2025-02-28 23:59:59.9999999", "2025-03-01 00:00:00.000000"}}},

};

namespace doris::vectorized {
// // make sure micro-seconds part of datetime has correct round behaviour
TEST(DatetimeRountTest, test_datetime_round_behaviour_basic) {
    {
        for (auto itr : scale_with_input_and_expected) {
            for (const auto& [input, expected] : itr.second) {
                from_string_checker(itr.first, input, expected);
            }
        }
    }
}

TEST(DatetimeRountTest, test_datetime_round_behaviour_get_field) {
    {
        for (auto itr : scale_with_input_and_expected) {
            for (const auto& [input, expected] : itr.second) {
                from_thrift_checker(itr.first, input, expected);
            }
        }
    }
}

TEST(DatetimeRountTest, test_datetime_round_behaviour_serialize) {
    {
        for (auto itr : scale_with_input_and_expected) {
            for (const auto& [input, expected] : itr.second) {
                serialization_checker(itr.first, input, expected);
            }
        }
    }
}
} // namespace doris::vectorized
