/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Author/Developer(s): Harihara Kadayam
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/

#include <gtest/gtest.h>
#include <random>
#include <map>
#include <memory>

#include "options/options.h"
#include "logging/logging.h"
#include "simple_node.hpp"
#include "varlen_node.hpp"
#include "utility/enum.hpp"
#include "btree_test_kvs.hpp"
#include "../mem_btree.hpp"

static constexpr uint32_t g_node_size{4096};
using namespace sisl::btree;
SISL_LOGGING_INIT(btree)

SISL_OPTIONS_ENABLE(logging, test_mem_btree)
SISL_OPTION_GROUP(test_btree_node,
                  (num_iters, "", "num_iters", "number of iterations for rand ops",
                   ::cxxopts::value< uint32_t >()->default_value("65536"), "number"))

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging, test_btree_node)
    sisl::logging::SetLogger("test_mem_btree");
    spdlog::set_pattern("[%D %T%z] [%^%L%$] [%t] %v");

    BtreeConfig m_cfg{g_node_size};
    MemBtree< TestFixedKey, TestFixedValue > bt{m_cfg};
    return ret;
}