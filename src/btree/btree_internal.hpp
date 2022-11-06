#pragma once

#include <boost/preprocessor/control/if.hpp>
#include <boost/preprocessor/facilities/empty.hpp>
#include <boost/preprocessor/facilities/identity.hpp>
#include <boost/vmd/is_empty.hpp>
#include "fds/utils.hpp"

namespace sisl {
namespace btree {

#define _BT_LOG_METHOD_IMPL(req, btcfg, node)                                                                          \
    ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {                                         \
        fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                                              \
                        fmt::make_format_args(file_name(__FILE__), __LINE__));                                         \
        BOOST_PP_IF(BOOST_VMD_IS_EMPTY(req), BOOST_PP_EMPTY,                                                           \
                    BOOST_PP_IDENTITY(fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[req={}] "},               \
                                                      fmt::make_format_args(req->to_string()))))                       \
        ();                                                                                                            \
        BOOST_PP_IF(BOOST_VMD_IS_EMPTY(btcfg), BOOST_PP_EMPTY,                                                         \
                    BOOST_PP_IDENTITY(fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[btree={}] "},             \
                                                      fmt::make_format_args(btcfg.name()))))                           \
        ();                                                                                                            \
        BOOST_PP_IF(BOOST_VMD_IS_EMPTY(node), BOOST_PP_EMPTY,                                                          \
                    BOOST_PP_IDENTITY(fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[node={}] "},              \
                                                      fmt::make_format_args(node->to_string()))))                      \
        ();                                                                                                            \
        fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                                                   \
                        fmt::make_format_args(std::forward< decltype(args) >(args)...));                               \
        return true;                                                                                                   \
    })

#define BT_LOG(level, msg, ...)                                                                                        \
    { LOG##level##MOD_FMT(btree, (_BT_LOG_METHOD_IMPL(, this->m_bt_cfg, )), msg, ##__VA_ARGS__); }

#define BT_NODE_LOG(level, node, msg, ...)                                                                             \
    { LOG##level##MOD_FMT(btree, (_BT_LOG_METHOD_IMPL(, this->m_bt_cfg, node)), msg, ##__VA_ARGS__); }

#if 0
#define THIS_BT_LOG(level, req, msg, ...)                                                                              \
    {                                                                                                                  \
        LOG##level##MOD_FMT(                                                                                           \
            btree, ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {                          \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                                      \
                                fmt::make_format_args(file_name(__FILE__), __LINE__));                                 \
                BOOST_PP_IF(BOOST_VMD_IS_EMPTY(req), BOOST_PP_EMPTY,                                                   \
                            BOOST_PP_IDENTITY(fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[req={}] "},       \
                                                              fmt::make_format_args(req->to_string()))))               \
                ();                                                                                                    \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[btree={}] "},                                   \
                                fmt::make_format_args(m_cfg.name()));                                                  \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                                           \
                                fmt::make_format_args(std::forward< decltype(args) >(args)...));                       \
                return true;                                                                                           \
            }),                                                                                                        \
            msg, ##__VA_ARGS__);                                                                                       \
    }

#define THIS_NODE_LOG(level, btcfg, msg, ...)                                                                          \
    {                                                                                                                  \
        LOG##level##MOD_FMT(                                                                                           \
            btree, ([&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {                          \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[{}:{}] "},                                      \
                                fmt::make_format_args(file_name(__FILE__), __LINE__));                                 \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[btree={}] "},                                   \
                                fmt::make_format_args(btcfg.name()));                                                  \
                BOOST_PP_IF(BOOST_VMD_IS_EMPTY(req), BOOST_PP_EMPTY,                                                   \
                            BOOST_PP_IDENTITY(fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[node={}] "},      \
                                                              fmt::make_format_args(to_string()))))                    \
                ();                                                                                                    \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                                           \
                                fmt::make_format_args(std::forward< decltype(args) >(args)...));                       \
                return true;                                                                                           \
            }),                                                                                                        \
            msg, ##__VA_ARGS__);                                                                                       \
    }

#define BT_ASSERT(assert_type, cond, req, ...)                                                                         \
    {                                                                                                                  \
        assert_type##_ASSERT_FMT(                                                                                      \
            cond,                                                                                                      \
            [&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {                                  \
                BOOST_PP_IF(BOOST_VMD_IS_EMPTY(req), BOOST_PP_EMPTY,                                                   \
                            BOOST_PP_IDENTITY(fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"\n[req={}] "},     \
                                                              fmt::make_format_args(req->to_string()))))               \
                ();                                                                                                    \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[btree={}] "},                                   \
                                fmt::make_format_args(m_cfg.name()));                                                  \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                                           \
                                fmt::make_format_args(std::forward< decltype(args) >(args)...));                       \
                return true;                                                                                           \
            },                                                                                                         \
            msg, ##__VA_ARGS__);                                                                                       \
    }

#define BT_ASSERT_CMP(assert_type, val1, cmp, val2, req, ...)                                                          \
    {                                                                                                                  \
        assert_type##_ASSERT_CMP(                                                                                      \
            val1, cmp, val2,                                                                                           \
            [&](fmt::memory_buffer& buf, const char* msgcb, auto&&... args) -> bool {                                  \
                BOOST_PP_IF(BOOST_VMD_IS_EMPTY(req), BOOST_PP_EMPTY,                                                   \
                            BOOST_PP_IDENTITY(fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"\n[req={}] "},     \
                                                              fmt::make_format_args(req->to_string()))))               \
                ();                                                                                                    \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{"[btree={}] "},                                   \
                                fmt::make_format_args(m_cfg.name()));                                                  \
                fmt::vformat_to(fmt::appender{buf}, fmt::string_view{msgcb},                                           \
                                fmt::make_format_args(std::forward< decltype(args) >(args)...));                       \
                return true;                                                                                           \
            },                                                                                                         \
            msg, ##__VA_ARGS__);                                                                                       \
    }
#endif

#define BT_ASSERT(assert_type, cond, ...)                                                                              \
    { assert_type##_ASSERT_FMT(cond, _BT_LOG_METHOD_IMPL(, this->m_bt_cfg, ), ##__VA_ARGS__); }

#define BT_ASSERT_CMP(assert_type, val1, cmp, val2, ...)                                                               \
    { assert_type##_ASSERT_CMP(val1, cmp, val2, _BT_LOG_METHOD_IMPL(, this->m_bt_cfg, ), ##__VA_ARGS__); }

#define BT_DBG_ASSERT(cond, ...) BT_ASSERT(DEBUG, cond, ##__VA_ARGS__)
#define BT_DBG_ASSERT_EQ(val1, val2, ...) BT_ASSERT_CMP(DEBUG, val1, ==, val2, ##__VA_ARGS__)
#define BT_DBG_ASSERT_NE(val1, val2, ...) BT_ASSERT_CMP(DEBUG, val1, !=, val2, ##__VA_ARGS__)
#define BT_DBG_ASSERT_LT(val1, val2, ...) BT_ASSERT_CMP(DEBUG, val1, <, val2, ##__VA_ARGS__)
#define BT_DBG_ASSERT_LE(val1, val2, ...) BT_ASSERT_CMP(DEBUG, val1, <=, val2, ##__VA_ARGS__)
#define BT_DBG_ASSERT_GT(val1, val2, ...) BT_ASSERT_CMP(DEBUG, val1, >, val2, ##__VA_ARGS__)
#define BT_DBG_ASSERT_GE(val1, val2, ...) BT_ASSERT_CMP(DEBUG, val1, >=, val2, ##__VA_ARGS__)

#define BT_LOG_ASSERT(cond, ...) BT_ASSERT(LOGMSG, cond, ##__VA_ARGS__)
#define BT_LOG_ASSERT_EQ(val1, val2, ...) BT_ASSERT_CMP(LOGMSG, val1, ==, val2, ##__VA_ARGS__)
#define BT_LOG_ASSERT_NE(val1, val2, ...) BT_ASSERT_CMP(LOGMSG, val1, !=, val2, ##__VA_ARGS__)
#define BT_LOG_ASSERT_LT(val1, val2, ...) BT_ASSERT_CMP(LOGMSG, val1, <, val2, ##__VA_ARGS__)
#define BT_LOG_ASSERT_LE(val1, val2, ...) BT_ASSERT_CMP(LOGMSG, val1, <=, val2, ##__VA_ARGS__)
#define BT_LOG_ASSERT_GT(val1, val2, ...) BT_ASSERT_CMP(LOGMSG, val1, >, val2, ##__VA_ARGS__)
#define BT_LOG_ASSERT_GE(val1, val2, ...) BT_ASSERT_CMP(LOGMSG, val1, >=, val2, ##__VA_ARGS__)

#define BT_REL_ASSERT(cond, ...) BT_ASSERT(RELEASE, cond, ##__VA_ARGS__)
#define BT_REL_ASSERT_EQ(val1, val2, ...) BT_ASSERT_CMP(RELEASE, val1, ==, val2, ##__VA_ARGS__)
#define BT_REL_ASSERT_NE(val1, val2, ...) BT_ASSERT_CMP(RELEASE, val1, !=, val2, ##__VA_ARGS__)
#define BT_REL_ASSERT_LT(val1, val2, ...) BT_ASSERT_CMP(RELEASE, val1, <, val2, ##__VA_ARGS__)
#define BT_REL_ASSERT_LE(val1, val2, ...) BT_ASSERT_CMP(RELEASE, val1, <=, val2, ##__VA_ARGS__)
#define BT_REL_ASSERT_GT(val1, val2, ...) BT_ASSERT_CMP(RELEASE, val1, >, val2, ##__VA_ARGS__)
#define BT_REL_ASSERT_GE(val1, val2, ...) BT_ASSERT_CMP(RELEASE, val1, >=, val2, ##__VA_ARGS__)

#define BT_NODE_ASSERT(assert_type, cond, node, ...)                                                                   \
    { assert_type##_ASSERT_FMT(cond, _BT_LOG_METHOD_IMPL(, m_bt_cfg, node), ##__VA_ARGS__); }

#define BT_NODE_ASSERT_CMP(assert_type, val1, cmp, val2, node, ...)                                                    \
    { assert_type##_ASSERT_CMP(val1, cmp, val2, _BT_LOG_METHOD_IMPL(, m_bt_cfg, node), ##__VA_ARGS__); }

#define BT_NODE_DBG_ASSERT(cond, ...) BT_NODE_ASSERT(DEBUG, cond, ##__VA_ARGS__)
#define BT_NODE_DBG_ASSERT_EQ(val1, val2, ...) BT_NODE_ASSERT_CMP(DEBUG, val1, ==, val2, ##__VA_ARGS__)
#define BT_NODE_DBG_ASSERT_NE(val1, val2, ...) BT_NODE_ASSERT_CMP(DEBUG, val1, !=, val2, ##__VA_ARGS__)
#define BT_NODE_DBG_ASSERT_LT(val1, val2, ...) BT_NODE_ASSERT_CMP(DEBUG, val1, <, val2, ##__VA_ARGS__)
#define BT_NODE_DBG_ASSERT_LE(val1, val2, ...) BT_NODE_ASSERT_CMP(DEBUG, val1, <=, val2, ##__VA_ARGS__)
#define BT_NODE_DBG_ASSERT_GT(val1, val2, ...) BT_NODE_ASSERT_CMP(DEBUG, val1, >, val2, ##__VA_ARGS__)
#define BT_NODE_DBG_ASSERT_GE(val1, val2, ...) BT_NODE_ASSERT_CMP(DEBUG, val1, >=, val2, ##__VA_ARGS__)

#define BT_NODE_LOG_ASSERT(cond, ...) BT_NODE_ASSERT(LOGMSG, cond, ##__VA_ARGS__)
#define BT_NODE_LOG_ASSERT_EQ(val1, val2, ...) BT_NODE_ASSERT_CMP(LOGMSG, val1, ==, val2, ##__VA_ARGS__)
#define BT_NODE_LOG_ASSERT_NE(val1, val2, ...) BT_NODE_ASSERT_CMP(LOGMSG, val1, !=, val2, ##__VA_ARGS__)
#define BT_NODE_LOG_ASSERT_LT(val1, val2, ...) BT_NODE_ASSERT_CMP(LOGMSG, val1, <, val2, ##__VA_ARGS__)
#define BT_NODE_LOG_ASSERT_LE(val1, val2, ...) BT_NODE_ASSERT_CMP(LOGMSG, val1, <=, val2, ##__VA_ARGS__)
#define BT_NODE_LOG_ASSERT_GT(val1, val2, ...) BT_NODE_ASSERT_CMP(LOGMSG, val1, >, val2, ##__VA_ARGS__)
#define BT_NODE_LOG_ASSERT_GE(val1, val2, ...) BT_NODE_ASSERT_CMP(LOGMSG, val1, >=, val2, ##__VA_ARGS__)

#define BT_NODE_REL_ASSERT(cond, ...) BT_NODE_ASSERT(RELEASE, cond, ##__VA_ARGS__)
#define BT_NODE_REL_ASSERT_EQ(val1, val2, ...) BT_NODE_ASSERT_CMP(RELEASE, val1, ==, val2, ##__VA_ARGS__)
#define BT_NODE_REL_ASSERT_NE(val1, val2, ...) BT_NODE_ASSERT_CMP(RELEASE, val1, !=, val2, ##__VA_ARGS__)
#define BT_NODE_REL_ASSERT_LT(val1, val2, ...) BT_NODE_ASSERT_CMP(RELEASE, val1, <, val2, ##__VA_ARGS__)
#define BT_NODE_REL_ASSERT_LE(val1, val2, ...) BT_NODE_ASSERT_CMP(RELEASE, val1, <=, val2, ##__VA_ARGS__)
#define BT_NODE_REL_ASSERT_GT(val1, val2, ...) BT_NODE_ASSERT_CMP(RELEASE, val1, >, val2, ##__VA_ARGS__)
#define BT_NODE_REL_ASSERT_GE(val1, val2, ...) BT_NODE_ASSERT_CMP(RELEASE, val1, >=, val2, ##__VA_ARGS__)

#define ASSERT_IS_VALID_INTERIOR_CHILD_INDX(is_found, found_idx, node)                                                 \
    BT_NODE_DBG_ASSERT((!is_found || ((int)found_idx < (int)node->total_entries()) || node->has_valid_edge()), node,   \
                       "Is_valid_interior_child_check_failed: found_idx={}", found_idx)

using bnodeid_t = uint64_t;
static constexpr bnodeid_t empty_bnodeid = std::numeric_limits< bnodeid_t >::max();
static constexpr uint16_t init_crc_16 = 0x8005;

VENUM(btree_node_type, uint32_t, FIXED = 0, VAR_VALUE = 1, VAR_KEY = 2, VAR_OBJECT = 3, PREFIX = 4, COMPACT = 5)

#ifdef USE_STORE_TYPE
VENUM(btree_store_type, uint8_t, MEM = 0, SSD = 1)
#endif

ENUM(btree_status_t, uint32_t, success, not_found, retry, has_more, read_failed, write_failed, stale_buf,
     refresh_failed, put_failed, space_not_avail, split_failed, insert_failed, cp_mismatch, merge_not_required,
     merge_failed, replay_not_needed, fast_path_not_possible, resource_full, crc_mismatch, not_supported, node_freed)

struct BtreeConfig {
    uint64_t m_max_objs{0};
    uint32_t m_max_key_size{0};
    uint32_t m_max_value_size{0};
    uint32_t m_node_size;

    uint8_t m_ideal_fill_pct{90};
    uint8_t m_split_pct{50};
    uint32_t m_rebalance_max_nodes{3};
    uint32_t m_rebalance_turned_on{false};

    bool m_custom_kv{false}; // If Key/Value needs some transformation before read or write
    btree_node_type m_leaf_node_type{btree_node_type::VAR_OBJECT};
    btree_node_type m_int_node_type{btree_node_type::VAR_KEY};
    std::string m_btree_name; // Unique name for the btree

    BtreeConfig(uint32_t node_size, const std::string& btree_name = "") :
            m_node_size{node_size}, m_btree_name{btree_name.empty() ? std::string("btree") : btree_name} {}

    virtual ~BtreeConfig() = default;
    uint32_t node_size() const { return m_node_size; };
    uint32_t max_key_size() const { return m_max_key_size; }
    void set_max_key_size(uint32_t max_key_size) { m_max_key_size = max_key_size; }

    uint64_t max_objs() const { return m_max_objs; }
    void set_max_objs(uint64_t max_objs) { m_max_objs = max_objs; }

    uint32_t max_value_size() const { return m_max_value_size; }

    void set_max_value_size(uint32_t max_value_size) { m_max_value_size = max_value_size; }

    uint32_t split_size(uint32_t filled_size) const { return uint32_cast(filled_size * m_split_pct) / 100; }
    uint32_t ideal_fill_size() const { return (uint32_t)(m_node_size * m_ideal_fill_pct) / 100; }
    const std::string& name() const { return m_btree_name; }

    bool is_custom_kv() const { return m_custom_kv; }
    btree_node_type leaf_node_type() const { return m_leaf_node_type; }
    btree_node_type interior_node_type() const { return m_int_node_type; }
};

class BtreeMetrics : public MetricsGroup {
public:
    explicit BtreeMetrics(const char* inst_name) : MetricsGroup("Btree", inst_name) {
        REGISTER_COUNTER(btree_obj_count, "Btree object count", _publish_as::publish_as_gauge);
        REGISTER_COUNTER(btree_leaf_node_count, "Btree Leaf node count", "btree_node_count", {"node_type", "leaf"},
                         _publish_as::publish_as_gauge);
        REGISTER_COUNTER(btree_int_node_count, "Btree Interior node count", "btree_node_count",
                         {"node_type", "interior"}, _publish_as::publish_as_gauge);
        REGISTER_COUNTER(btree_split_count, "Total number of btree node splits");
        REGISTER_COUNTER(insert_failed_count, "Total number of inserts failed");
        REGISTER_COUNTER(btree_merge_count, "Total number of btree node merges");
        REGISTER_COUNTER(btree_depth, "Depth of btree", _publish_as::publish_as_gauge);

        REGISTER_COUNTER(btree_int_node_writes, "Total number of btree interior node writes", "btree_node_writes",
                         {"node_type", "interior"});
        REGISTER_COUNTER(btree_leaf_node_writes, "Total number of btree leaf node writes", "btree_node_writes",
                         {"node_type", "leaf"});
        REGISTER_COUNTER(btree_num_pc_gen_mismatch, "Number of gen mismatches to recover");

        REGISTER_HISTOGRAM(btree_int_node_occupancy, "Interior node occupancy", "btree_node_occupancy",
                           {"node_type", "interior"}, HistogramBucketsType(LinearUpto128Buckets));
        REGISTER_HISTOGRAM(btree_leaf_node_occupancy, "Leaf node occupancy", "btree_node_occupancy",
                           {"node_type", "leaf"}, HistogramBucketsType(LinearUpto128Buckets));
        REGISTER_COUNTER(btree_retry_count, "number of retries");
        REGISTER_COUNTER(write_err_cnt, "number of errors in write");
        REGISTER_COUNTER(split_failed, "split failed");
        REGISTER_COUNTER(query_err_cnt, "number of errors in query");
        REGISTER_COUNTER(read_node_count_in_write_ops, "number of nodes read in write_op");
        REGISTER_COUNTER(read_node_count_in_query_ops, "number of nodes read in query_op");
        REGISTER_COUNTER(btree_write_ops_count, "number of btree operations");
        REGISTER_COUNTER(btree_query_ops_count, "number of btree operations");
        REGISTER_COUNTER(btree_remove_ops_count, "number of btree operations");
        REGISTER_HISTOGRAM(btree_exclusive_time_in_int_node,
                           "Exclusive time spent (Write locked) on interior node (ns)", "btree_exclusive_time_in_node",
                           {"node_type", "interior"});
        REGISTER_HISTOGRAM(btree_exclusive_time_in_leaf_node, "Exclusive time spent (Write locked) on leaf node (ns)",
                           "btree_exclusive_time_in_node", {"node_type", "leaf"});
        REGISTER_HISTOGRAM(btree_inclusive_time_in_int_node, "Inclusive time spent (Read locked) on interior node (ns)",
                           "btree_inclusive_time_in_node", {"node_type", "interior"});
        REGISTER_HISTOGRAM(btree_inclusive_time_in_leaf_node, "Inclusive time spent (Read locked) on leaf node (ns)",
                           "btree_inclusive_time_in_node", {"node_type", "leaf"});

        register_me_to_farm();
    }

    ~BtreeMetrics() { deregister_me_from_farm(); }
};

} // namespace btree
} // namespace sisl
