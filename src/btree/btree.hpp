/*
 *  Created on: 14-May-2016
 *      Author: Hari Kadayam
 *
 *  Copyright � 2016 Kadayam, Hari. All rights reserved.
 */

#include <atomic>
#include <array>

#include "btree_internal.hpp"
#include "btree_req.hpp"
#include "btree_kv.hpp"

namespace sisl {
namespace btree {

template < typename K, typename V >
class BtreeCommon {
public:
    void deref_node(BtreeNode< K, V >* node) = 0;
};

template < typename K, typename V >
class Btree {
private:
    mutable folly::SharedMutexWritePriority m_btree_lock;
    bnodeid_t m_root_node_id{empty_bnodeid};
    uint32_t m_max_nodes;
    BtreeConfig m_btree_cfg;

    BtreeMetrics m_metrics;
    bool m_destroy{false};
    std::atomic< uint64_t > m_total_nodes{0};
    uint32_t m_node_size{4096};
#ifndef NDEBUG
    std::atomic< uint64_t > m_req_id{0};
#endif

public:
    static Btree< K, V >* create_btree(const BtreeConfig& cfg);

    /////////////////////////////////////// All External APIs /////////////////////////////
    Btree(const BtreeConfig& cfg);
    virtual ~Btree();
    btree_status_t put(const BtreeKey& k, const BtreeValue& v, btree_put_type put_type, BtreeValue* existing_val);
    btree_status_t range_put(BtreeRangeUpdateRequest< K, V >& bur);
    btree_status_t get(const BtreeKey& key, BtreeValue* outval) const;
    btree_status_t get(const BtreeKey& key, BtreeKey* outkey, BtreeValue* outval) const;
    btree_status_t get_any(const BtreeKeyRangeSafe< K >& range, BtreeKey* outkey, BtreeValue* outval) const;
    btree_status_t remove_any(const BtreeKeyRangeSafe< K >& range, BtreeKey* outkey, BtreeValue* outval);
    btree_status_t remove(const BtreeKey& key, BtreeValue* outval);
    btree_status_t query(BtreeQueryRequest< K, V >& query_req, std::vector< std::pair< K, V > >& out_values) const;
    bool verify_tree(bool update_debug_bm);
    nlohmann::json get_status(int log_level) const;
    void print_tree() const;
    nlohmann::json get_metrics_in_json(bool updated = true) const;

    static void set_io_flip();
    static void set_error_flip();

    // static std::array< std::shared_ptr< BtreeCommon< K, V > >, sizeof(btree_stores_t) > s_btree_stores;
    // static std::mutex s_store_reg_mtx;

private:
    /////////////////////////////////// Node Management calls ///////////////////////////////////////
    btree_status_t create_root_node(bnodeid_t& out_root_node);
    btree_status_t read_and_lock_root(bnodeid_t id, BtreeNodePtr& node_ptr, locktype_t int_lock_type,
                                      locktype_t leaf_lock_type, void* context) const;
    btree_status_t read_and_lock_child(bnodeid_t child_id, BtreeNodePtr& child_node, const BtreeNodePtr& parent_node,
                                       uint32_t parent_ind, locktype_t int_lock_type, locktype_t leaf_lock_type) const;
    btree_status_t read_and_lock_sibling(bnodeid_t id, BtreeNodePtr& node_ptr, locktype_t int_lock_type,
                                         locktype_t leaf_lock_type) const;
    btree_status_t read_and_lock_node(bnodeid_t id, BtreeNodePtr& node_ptr, locktype_t int_lock_type,
                                      locktype_t leaf_lock_type) const;
    btree_status_t get_child_and_lock_node(const BtreeNodePtr& node, uint32_t index, BtreeNodeInfo& child_info,
                                           BtreeNodePtr& child_node, locktype_t int_lock_type,
                                           locktype_t leaf_lock_type) const;
    btree_status_t write_node_sync(const BtreeNodePtr& node);
    btree_status_t write_node(const BtreeNodePtr& node);
    btree_status_t write_node(const BtreeNodePtr& node, const BtreeNodePtr& dependent_node);
    btree_status_t upgrade_node(const BtreeNodePtr& my_node, BtreeNodePtr child_node, void* context,
                                locktype_t& cur_lock, locktype_t& child_cur_lock);
    void read_node_or_fail(bnodeid_t id, BtreeNodePtr& node) const;
    btree_status_t read_node(bnodeid_t id, BtreeNodePtr& node) const;
    btree_status_t _lock_and_refresh_node(const BtreeNodePtr& node, locktype_t type, void* context, const char* fname,
                                          int line) const;
    btree_status_t _lock_node_upgrade(const BtreeNodePtr& node, void* context, const char* fname, int line);
    void unlock_node(const BtreeNodePtr& node, locktype_t type) const;
    BtreeNodePtr alloc_leaf_node();
    BtreeNodePtr alloc_interior_node();
    BtreeNode< K, V >* init_node(uint8_t* node_buf, bnodeid_t id, bool init_buf, bool is_leaf);
    void free_node(const BtreeNodePtr& node);
    void observe_lock_time(const BtreeNodePtr& node, locktype_t type, uint64_t time_spent) const;

    static void _start_of_lock(const BtreeNodePtr& node, locktype_t ltype, const char* fname, int line);
    static bool remove_locked_node(const BtreeNodePtr& node, locktype_t ltype, btree_locked_node_info* out_info);
    static uint64_t end_of_lock(const BtreeNodePtr& node, locktype_t ltype);
#ifndef NDEBUG
    static void check_lock_debug();
#endif

protected:
    virtual BtreeNodePtr alloc_node(bool is_leaf, bool& is_new_allocation,
                                    const BtreeNodePtr& copy_from = nullptr) const = 0;
    virtual btree_status_t read_node(bnodeid_t id, BtreeNodePtr& bnode) const = 0;
    virtual btree_status_t write_node(const BtreeNodePtr& bn, const BtreeNodePtr& dependent_bn,
                                      const BtreeRequest& req) override = 0;
    virtual void swap_node(const BtreeNodePtr& node1, const BtreeNodePtr& node2) = 0;
    virtual void btree_status_t refresh_node(const BtreeNodePtr& bn, bool is_write_modifiable) = 0;

    virtual void split_node_precommit(const BtreeNodePtr& parent_node, const BtreeNodePtr& child_node1,
                                      const BtreeNodePtr& child_node2, bool root_split, void* op_context) = 0;
    virtual void merge_node_precommit(bool is_root_merge, const BtreeNodePtr& parent_node,
                                      uint32_t parent_merge_start_idx, const BtreeNodePtr& child_node1,
                                      const std::vector< BtreeNodePtr >* old_child_nodes,
                                      const std::vector< BtreeNodePtr >* replace_child_nodes, void* op_context) = 0;

    virtual int64_t compute_single_put_needed_size(const V& current_val, const V& new_val) const;
    virtual int64_t compute_range_put_needed_size(const std::vector< std::pair< K, V > >& existing_kvs,
                                                  const V& new_val) const;
    virtual btree_status_t custom_kv_select_for_write(uint8_t node_version,
                                                      const std::vector< std::pair< K, V > >& match_kv,
                                                      std::vector< std::pair< K, V > >& replace_kv,
                                                      const BtreeKeyRange& range, const BtreeRangeRequest& rureq) const;

    virtual btree_status_t custom_kv_select_for_read(uint8_t node_version,
                                                     const std::vector< std::pair< K, V > >& match_kv,
                                                     std::vector< std::pair< K, V > >& replace_kv,
                                                     const BtreeKeyRange& range, const BtreeRangeRequest& qreq) const;
};
} // namespace btree
} // namespace sisl
