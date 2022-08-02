#pragma once
#include "btree_kv.hpp"
#include "fds/buffer.hpp"

namespace sisl {

struct BtreeRequest {
    void* m_app_context{nullptr};
    void* m_op_context{nullptr};
};

/////////////////////////// 1: Single Operations /////////////////////////////////////
struct BtreeSinglePutRequest : public BtreeRequest {
public:
    BtreeSinglePutRequest(const BtreeKey& k, BtreeValue& v, btree_put_type put_type,
                          BtreeValue* existing_val = nullptr) :
            m_k{k}, m_v{v}, m_put_type{put_type}, m_existing_val{existing_val} {}

    BtreeKey& m_k;
    BtreeValue& m_v;
    btree_put_type m_put_type;
    BtreeValue* m_existing_val;
};

struct BtreeGetRequest : public BtreeRequest {
public:
    BtreeGetRequest(BtreeKeyRange&& range, BtreeValue* out_val, BtreeKey* outkey = nullptr) :
            m_keys{std::move(range)}, m_outkey{outkey}, m_outval{out_val} {}

    const BtreeKeyRange m_keys;
    BtreeKey* m_outkey{nullptr};
    BtreeValue* m_outval{nullptr};
};

struct BtreeRemoveRequest : public BtreeRequest {
public:
    BtreeRemoveRequest(BtreeKeyRange&& range, BtreeValue* out_val, BtreeKey* outkey = nullptr) :
            m_keys{std::move(range)}, m_outkey{outkey}, m_outval{out_val} {}

    const BtreeKeyRange m_keys;
    BtreeKey* m_outkey{nullptr};
    BtreeValue* m_outval{nullptr};
};

/////////////////////////// 2: Range Operations /////////////////////////////////////
// Base class for range requests
struct BtreeRangeRequest : public BtreeRequest {
public:
    const BtreeKeyRange& input_range() { return m_search_state.m_input_range; }
    uint32_t batch_size() const { return m_batch_size; }
    void set_batch_size(uint32_t count) { m_batch_size = count; }

    bool is_empty_cursor() const {
        return ((m_input_range->get_cur()->m_last_key == nullptr) &&
                (m_input_range->get_cur()->m_locked_nodes == nullptr));
    }

    BtreeSearchState& search_state() { return m_search_state; }
    BtreeQueryCursor* cursor() { return m_search_state.cursor(); }
    const BtreeKeyRange& next_range() const { return m_search_state.next_range(); }

    const BtreeKeyRange& current_sub_range() const { return m_search_state.m_current_sub_range; }
    void set_current_sub_range(const BtreeKeyRange& new_sub_range) {
        m_search_state.m_current_sub_range = new_sub_range;
    }
    const BtreeKey& next_key() const { return m_search_state.next_key(); }
    const BtreeKeyRange& next_start_range() const { return m_search_state.next_start_range(); }
    const BtreeKeyRange& end_of_range() const { return m_search_state.end_of_range(); }

protected:
    BtreeRangeRequest(BtreeSearchState&& search_state, void* app_context = nullptr, uint32_t batch_size = UINT32_MAX) :
            m_app_context{app_context}, m_search_state(std::move(search_state)), m_batch_size(UINT32_MAX) {}

private:
    BtreeSearchState m_search_state;
    uint32_t m_batch_size{1};
};

/////////////////////////// 2a Range Mutate Operations /////////////////////////////////////
struct BtreeRangeUpdateRequest : public BtreeRangeRequest {
public:
    BtreeRangeUpdateRequest(BtreeSearchState&& search_state, btree_put_type put_type,
                            const std::vector< BtreeValue* >& values, void* app_context = nullptr,
                            uint32_t batch_size = std::numeric_limits< uint32_t >::max()) :
            BtreeRangeRequest(std::move(search_state), app_context, batch_size),
            m_put_type{put_type},
            m_values{values} {}

    const btree_put_type m_put_type{btree_put_type::INSERT_ONLY_IF_NOT_EXISTS};
    const BtreeValue* m_value;
};

using BtreeMutateRequest = std::variant< BtreeSinglePutRequest, BtreeRangeUpdateRequest >;

static bool is_range_update_req(const BtreeMutateRequest& req) {
    return (std::holds_alternative< BtreeRangeUpdateRequest >(req));
}

static BtreeRangeUpdateRequest& to_range_update_req(const BtreeMutateRequest& req) {
    return std::get< BtreeRangeUpdateRequest& >(req);
}

static BtreeSinglePutRequest& to_single_put_req(const BtreeMutateRequest& req) {
    return std::get< BtreeSinglePutRequest& >(req);
}

/////////////////////////// 2a Range Query Operations /////////////////////////////////////
ENUM(BtreeQueryType, uint8_t,
     // This is default query which walks to first element in range, and then sweeps/walks
     // across the leaf nodes. However, if upon pagination, it again walks down the query from
     // the key it left off.
     SWEEP_NON_INTRUSIVE_PAGINATION_QUERY,

     // Similar to sweep query, except that it retains the node and its lock during
     // pagination. This is more of intrusive query and if the caller is not careful, the read
     // lock will never be unlocked and could cause deadlocks. Use this option carefully.
     SWEEP_INTRUSIVE_PAGINATION_QUERY,

     // This is relatively inefficient query where every leaf node goes from its parent node
     // instead of walking the leaf node across. This is useful only if we want to check and
     // recover if parent and leaf node are in different generations or crash recovery cases.
     TREE_TRAVERSAL_QUERY,

     // This is both inefficient and quiet intrusive/unsafe query, where it locks the range
     // that is being queried for and do not allow any insert or update within that range. It
     // essentially create a serializable level of isolation.
     SERIALIZABLE_QUERY)

template < typename K, typename V >
struct BtreeQueryRequest : public BtreeRangeRequest {
public:
    /* TODO :- uint32_max to c++. pass reference */
    BtreeQueryRequest(BtreeSearchState&& search_state,
                      BtreeQueryType query_type = BtreeQueryType::SWEEP_NON_INTRUSIVE_PAGINATION_QUERY,
                      uint32_t batch_size = UINT32_MAX, void* app_context = nullptr) :
            BtreeRangeRequest(std::move(search_state), app_context, batch_size), m_query_type(query_type) {}
    ~BtreeQueryRequest() = default;

    // virtual bool is_serializable() const = 0;
    BtreeQueryType query_type() const { return m_query_type; }

protected:
    const BtreeQueryType m_query_type; // Type of the query
};

/* This class is a top level class to keep track of the locks that are held currently. It is
 * used for serializabke query to unlock all nodes in right order at the end of the lock */
class BtreeLockTracker {
public:
    virtual ~BtreeLockTracker() = default;
};

#if 0
class BtreeSweepQueryRequest : public BtreeQueryRequest {
public:
    BtreeSweepQueryRequest(const BtreeSearchRange& criteria, uint32_t iter_count = 1000,
            const match_item_cb_t& match_item_cb = nullptr) :
            BtreeQueryRequest(criteria, iter_count, match_item_cb) {}

    BtreeSweepQueryRequest(const BtreeSearchRange &criteria, const match_item_cb_t& match_item_cb) :
            BtreeQueryRequest(criteria, 1000, match_item_cb) {}

    bool is_serializable() const { return false; }
};

class BtreeSerializableQueryRequest : public BtreeQueryRequest {
public:
    BtreeSerializableQueryRequest(const BtreeSearchRange &range, uint32_t iter_count = 1000,
                             const match_item_cb_t& match_item_cb = nullptr) :
            BtreeQueryRequest(range, iter_count, match_item_cb) {}

    BtreeSerializableQueryRequest(const BtreeSearchRange &criteria, const match_item_cb_t& match_item_cb) :
            BtreeSerializableQueryRequest(criteria, 1000, match_item_cb) {}

    bool is_serializable() const { return true; }
};
#endif
} // namespace sisl
