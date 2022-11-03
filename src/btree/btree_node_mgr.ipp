#pragma once

#include "btree.hpp"
#include "../fds/utils.hpp"
#include <chrono>

namespace sisl {
namespace btree {

#define lock_node(a, b, c) _lock_node(a, b, c, __FILE__, __LINE__)

template < typename K, typename V >
std::pair< btree_status_t, bnodeid_t > Btree< K, V >::create_root_node(void* op_context) {
    // Assign one node as root node and also create a child leaf node and set it as edge
    BtreeNodePtr< K > root = alloc_interior_node();
    if (root == nullptr) { return std::make_pair(btree_status_t::space_not_avail, empty_bnodeid); }

    BtreeNodePtr< K > child = alloc_leaf_node();
    if (child == nullptr) {
        free_node(root, locktype_t::NONE, op_context);
        return std::make_pair(btree_status_t::space_not_avail, empty_bnodeid);
    }

    m_root_node_id = root->get_node_id();

    // Add the newly created child node as edge of root
    root->set_edge_value(BtreeNodeInfo{child->get_node_id()});

    // TODO: Need to update create_tree method with 2 nodes (instead of just root)
    create_tree_precommit(root, op_context);

    auto ret = write_node(root, nullptr, op_context);
    BT_DBG_ASSERT_EQ(ret, btree_status_t::success, "Writing root node failed");

    if (ret == btree_status_t::success) {
        ret = write_node(child, nullptr, op_context);
        BT_DBG_ASSERT_EQ(ret, btree_status_t::success, "Writing first child node failed");
    }

    /* write an entry to the journal also */
    return std::make_pair(ret, m_root_node_id);
}

/*
 * It reads the node and take a lock of the node.
 */
template < typename K, typename V >
btree_status_t Btree< K, V >::read_and_lock_node(bnodeid_t id, BtreeNodePtr< K >& node_ptr, locktype_t int_lock_type,
                                                 locktype_t leaf_lock_type, void* context) const {
    auto ret = read_node_impl(id, node_ptr);
    if (node_ptr == nullptr) {
        if (ret != btree_status_t::fast_path_not_possible) { BT_LOG(ERROR, "read failed, reason: {}", ret); }
        return ret;
    }

    auto acq_lock = (node_ptr->is_leaf()) ? leaf_lock_type : int_lock_type;
    ret = lock_node(node_ptr, acq_lock, context);
    if (ret != btree_status_t::success) { BT_LOG(ERROR, "Node lock and refresh failed"); }

    return ret;
}

template < typename K, typename V >
btree_status_t Btree< K, V >::get_child_and_lock_node(const BtreeNodePtr< K >& node, uint32_t index,
                                                      BtreeNodeInfo& child_info, BtreeNodePtr< K >& child_node,
                                                      locktype_t int_lock_type, locktype_t leaf_lock_type,
                                                      void* context) const {
    if (index == node->get_total_entries()) {
        const auto& edge_id{node->get_edge_id()};
        child_info.set_bnode_id(edge_id);
        // If bsearch points to last index, it means the search has not found entry unless it is an edge value.
        if (!child_info.has_valid_bnode_id()) {
            BT_NODE_LOG_ASSERT(false, node, "Child index {} does not have valid bnode_id", index);
            return btree_status_t::not_found;
        }
    } else {
        BT_NODE_LOG_ASSERT_LT(index, node->get_total_entries(), node);
        node->get_nth_value(index, &child_info, false /* copy */);
    }

    return (read_and_lock_node(child_info.bnode_id(), child_node, int_lock_type, leaf_lock_type, context));
}

template < typename K, typename V >
btree_status_t Btree< K, V >::write_node(const BtreeNodePtr< K >& node, const BtreeNodePtr< K >& dep_node,
                                         void* context) {
    BT_NODE_LOG(DEBUG, node, "Writing node");

    COUNTER_INCREMENT_IF_ELSE(m_metrics, node->is_leaf(), btree_leaf_node_writes, btree_int_node_writes, 1);
    HISTOGRAM_OBSERVE_IF_ELSE(m_metrics, node->is_leaf(), btree_leaf_node_occupancy, btree_int_node_occupancy,
                              ((m_node_size - node->get_available_size(m_bt_cfg)) * 100) / m_node_size);

    return (write_node_impl(node, dep_node, context));
}

/* Caller of this api doesn't expect read to fail in any circumstance */
template < typename K, typename V >
void Btree< K, V >::read_node_or_fail(bnodeid_t id, BtreeNodePtr< K >& node) const {
    BT_NODE_REL_ASSERT_EQ(read_node_impl(id, node), btree_status_t::success, node);
}

/*
 * This function upgrades the parent node and child node locks from read lock to write lock and take required steps if
 * things have changed during the upgrade.
 *
 * Inputs:
 * parent_node - Parent Node to upgrade
 * child_node - Child Node to upgrade
 * child_cur_lock - Current child node which is held
 * context - Context to pass down
 *
 * Returns - If successfully able to upgrade both the nodes, return success, else return status of upgrade_node.
 * In case of not success, all nodes locks are released.
 *
 * NOTE: This function expects both the parent_node and child_node to be already locked. Parent node is
 * expected to be read locked and child node could be either read or write locked.
 */
template < typename K, typename V >
btree_status_t Btree< K, V >::upgrade_node_locks(const BtreeNodePtr< K >& parent_node,
                                                 const BtreeNodePtr< K >& child_node, locktype_t child_cur_lock,
                                                 void* context) {
    btree_status_t ret = btree_status_t::success;

    auto const parent_prev_gen = parent_node->get_gen();
    auto const child_prev_gen = child_node->get_gen();

    unlock_node(child_node, child_cur_lock);
    child_cur_lock = locktype_t::NONE;

#if 0
#ifdef _PRERELEASE
    {
        auto time = homestore_flip->get_test_flip< uint64_t >("btree_upgrade_delay");
        if (time) { std::this_thread::sleep_for(std::chrono::microseconds{time.get()}); }
    }
#endif
#endif

    // Attempt to upgrade the parent node
    ret = upgrade_node(parent_node, locktype_t::READ, context, parent_prev_gen);
    if (ret != btree_status_t::success) { return ret; }

    // Now attempt to upgrade the child node
    ret = upgrade_node(child_node, locktype_t::NONE, context, child_prev_gen);
    if (ret != btree_status_t::success) {
        unlock_node(parent_node, locktype_t::WRITE);
        return ret;
    }

#if 0
#ifdef _PRERELEASE
    {
        int is_leaf = 0;

        if (child_node && child_node->is_leaf()) { is_leaf = 1; }
        if (homestore_flip->test_flip("btree_upgrade_node_fail", is_leaf)) {
            unlock_node(my_node, cur_lock);
            cur_lock = locktype_t::NONE;
            if (child_node) {
                unlock_node(child_node, child_cur_lock);
                child_cur_lock = locktype_t::NONE;
            }
            ret = btree_status_t::retry;
        }
    }
#endif
#endif

    return ret;
}

template < typename K, typename V >
btree_status_t Btree< K, V >::upgrade_node(const BtreeNodePtr< K >& node, locktype_t prev_lock, void* context,
                                           uint64_t prev_gen) {
    if (prev_lock == locktype_t::READ) { unlock_node(node, locktype_t::READ); }
    if (prev_lock != locktype_t::WRITE) {
        auto const ret = lock_node(node, locktype_t::WRITE, context);
        if (ret != btree_status_t::success) { return ret; }
    }

    // If the node has been made invalid (probably by merge nodes) ask caller to start over again, but before
    // that cleanup or free this node if there is no one waiting.
    if (!node->is_valid_node()) {
        unlock_node(node, locktype_t::WRITE);
        return btree_status_t::retry;
    }

    // If node has been updated, while we have upgraded, ask caller to start all over again.
    if (prev_gen != node->get_gen()) {
        unlock_node(node, locktype_t::WRITE);
        return btree_status_t::retry;
    }
    return btree_status_t::success;
}

template < typename K, typename V >
btree_status_t Btree< K, V >::_lock_node(const BtreeNodePtr< K >& node, locktype_t type, void* context,
                                         const char* fname, int line) const {
    _start_of_lock(node, type, fname, line);
    node->lock(type);

    auto ret = refresh_node(node, (type == locktype_t::WRITE), context);
    if (ret != btree_status_t::success) {
        node->unlock(type);
        return ret;
    }

    return btree_status_t::success;
}

template < typename K, typename V >
void Btree< K, V >::unlock_node(const BtreeNodePtr< K >& node, locktype_t type) const {
    node->unlock(type);
    auto time_spent = end_of_lock(node, type);
    observe_lock_time(node, type, time_spent);
}

template < typename K, typename V >
BtreeNodePtr< K > Btree< K, V >::alloc_leaf_node() {
    bool is_new_allocation;
    BtreeNodePtr< K > n = alloc_node(true /* is_leaf */, is_new_allocation);
    if (n) {
        COUNTER_INCREMENT(m_metrics, btree_leaf_node_count, 1);
        ++m_total_nodes;
    }
    return n;
}

template < typename K, typename V >
BtreeNodePtr< K > Btree< K, V >::alloc_interior_node() {
    bool is_new_allocation;
    BtreeNodePtr< K > n = alloc_node(false /* is_leaf */, is_new_allocation);
    if (n) {
        COUNTER_INCREMENT(m_metrics, btree_int_node_count, 1);
        ++m_total_nodes;
    }
    return n;
}

template < typename K, typename V >
BtreeNode< K >* Btree< K, V >::init_node(uint8_t* node_buf, bnodeid_t id, bool init_buf, bool is_leaf) {
    BtreeNode< K >* ret_node{nullptr};
    btree_node_type node_type = is_leaf ? m_bt_cfg.leaf_node_type() : m_bt_cfg.interior_node_type();

    switch (node_type) {
    case btree_node_type::VAR_OBJECT:
        if (is_leaf) {
            ret_node = new VarObjSizeNode< K, V >(node_buf, id, init_buf, is_leaf, this->m_bt_cfg);
        } else {
            ret_node = new VarObjSizeNode< K, BtreeNodeInfo >(node_buf, id, init_buf, is_leaf, this->m_bt_cfg);
        }
        break;

    case btree_node_type::FIXED:
        if (is_leaf) {
            ret_node = dynamic_cast< BtreeNode< K >* >(
                new SimpleNode< K, V >(node_buf, id, init_buf, is_leaf, this->m_bt_cfg));
        } else {
            ret_node = dynamic_cast< BtreeNode< K >* >(
                new SimpleNode< K, BtreeNodeInfo >(node_buf, id, init_buf, is_leaf, this->m_bt_cfg));
        }
        break;

    case btree_node_type::VAR_VALUE:
        if (is_leaf) {
            ret_node = new VarValueSizeNode< K, V >(node_buf, id, init_buf, is_leaf, this->m_bt_cfg);
        } else {
            ret_node = new VarValueSizeNode< K, BtreeNodeInfo >(node_buf, id, init_buf, is_leaf, this->m_bt_cfg);
        }
        break;

    case btree_node_type::VAR_KEY:
        if (is_leaf) {
            ret_node = dynamic_cast< BtreeNode< K >* >(
                new VarKeySizeNode< K, V >(node_buf, id, init_buf, is_leaf, this->m_bt_cfg));
        } else {
            ret_node = dynamic_cast< BtreeNode< K >* >(
                new VarKeySizeNode< K, BtreeNodeInfo >(node_buf, id, init_buf, is_leaf, this->m_bt_cfg));
        }
        break;

    default:
        BT_REL_ASSERT(false, "Unsupported node type {}", node_type);
        break;
    }
    return ret_node;
}

/* Note:- This function assumes that access of this node is thread safe. */
template < typename K, typename V >
void Btree< K, V >::free_node(const BtreeNodePtr< K >& node, locktype_t cur_lock, void* context) {
    BT_NODE_LOG(DEBUG, node, "Freeing node");

    COUNTER_DECREMENT_IF_ELSE(m_metrics, node->is_leaf(), btree_leaf_node_count, btree_int_node_count, 1);
    if (cur_lock != locktype_t::NONE) {
        BT_NODE_DBG_ASSERT_NE(cur_lock, locktype_t::READ, node, "We can't free a node with read lock type right?");
        node->set_valid_node(false);
        unlock_node(node, cur_lock);
    }
    --m_total_nodes;

    free_node_impl(node, context);
    intrusive_ptr_release(node.get());
}

template < typename K, typename V >
void Btree< K, V >::observe_lock_time(const BtreeNodePtr< K >& node, locktype_t type, uint64_t time_spent) const {
    if (time_spent == 0) { return; }

    if (type == locktype_t::READ) {
        HISTOGRAM_OBSERVE_IF_ELSE(m_metrics, node->is_leaf(), btree_inclusive_time_in_leaf_node,
                                  btree_inclusive_time_in_int_node, time_spent);
    } else {
        HISTOGRAM_OBSERVE_IF_ELSE(m_metrics, node->is_leaf(), btree_exclusive_time_in_leaf_node,
                                  btree_exclusive_time_in_int_node, time_spent);
    }
}

template < typename K, typename V >
void Btree< K, V >::_start_of_lock(const BtreeNodePtr< K >& node, locktype_t ltype, const char* fname, int line) {
    btree_locked_node_info< K, V > info;

#ifndef NDEBUG
    info.fname = fname;
    info.line = line;
#endif

    info.start_time = Clock::now();
    info.node = node.get();
    if (ltype == locktype_t::WRITE) {
        bt_thread_vars()->wr_locked_nodes.push_back(info);
        LOGTRACEMOD(btree, "ADDING node {} to write locked nodes list, its size={}", (void*)info.node,
                    bt_thread_vars()->wr_locked_nodes.size());
    } else if (ltype == locktype_t::READ) {
        bt_thread_vars()->rd_locked_nodes.push_back(info);
        LOGTRACEMOD(btree, "ADDING node {} to read locked nodes list, its size={}", (void*)info.node,
                    bt_thread_vars()->rd_locked_nodes.size());
    } else {
        DEBUG_ASSERT(false, "Invalid locktype_t {}", ltype);
    }
}

template < typename K, typename V >
bool Btree< K, V >::remove_locked_node(const BtreeNodePtr< K >& node, locktype_t ltype,
                                       btree_locked_node_info< K, V >* out_info) {
    auto pnode_infos =
        (ltype == locktype_t::WRITE) ? &bt_thread_vars()->wr_locked_nodes : &bt_thread_vars()->rd_locked_nodes;

    if (!pnode_infos->empty()) {
        auto info = pnode_infos->back();
        if (info.node == node.get()) {
            *out_info = info;
            pnode_infos->pop_back();
            LOGTRACEMOD(btree, "REMOVING node {} from {} locked nodes list, its size = {}", (void*)info.node,
                        (ltype == locktype_t::WRITE) ? "write" : "read", pnode_infos->size());
            return true;
        } else if (pnode_infos->size() > 1) {
            info = pnode_infos->at(pnode_infos->size() - 2);
            if (info.node == node.get()) {
                *out_info = info;
                pnode_infos->at(pnode_infos->size() - 2) = pnode_infos->back();
                pnode_infos->pop_back();
                LOGTRACEMOD(btree, "REMOVING node {} from {} locked nodes list, its size = {}", (void*)info.node,
                            (ltype == locktype_t::WRITE) ? "write" : "read", pnode_infos->size());
                return true;
            }
        }
    }

#ifndef NDEBUG
    if (pnode_infos->empty()) {
        LOGERRORMOD(btree, "locked_node_list: node = {} not found, locked node list empty", (void*)node.get());
    } else if (pnode_infos->size() == 1) {
        LOGERRORMOD(btree, "locked_node_list: node = {} not found, total list count = 1, Expecting node = {}",
                    (void*)node.get(), (void*)pnode_infos->back().node);
    } else {
        LOGERRORMOD(btree, "locked_node_list: node = {} not found, total list count = {}, Expecting nodes = {} or {}",
                    (void*)node.get(), pnode_infos->size(), (void*)pnode_infos->back().node,
                    (void*)pnode_infos->at(pnode_infos->size() - 2).node);
    }
#endif
    return false;
}

template < typename K, typename V >
uint64_t Btree< K, V >::end_of_lock(const BtreeNodePtr< K >& node, locktype_t ltype) {
    btree_locked_node_info< K, V > info;
    if (!remove_locked_node(node, ltype, &info)) {
        DEBUG_ASSERT(false, "Expected node = {} is not there in locked_node_list", (void*)node.get());
        return 0;
    }
    // DEBUG_ASSERT_EQ(node.get(), info.node);
    return get_elapsed_time_ns(info.start_time);
}

#ifndef NDEBUG
template < typename K, typename V >
void Btree< K, V >::check_lock_debug() {
    // both wr_locked_nodes and rd_locked_nodes are thread_local;
    // nothing will be dumpped if there is no assert failure;
    for (const auto& x : bt_thread_vars()->wr_locked_nodes) {
        x.dump();
    }
    for (const auto& x : bt_thread_vars()->rd_locked_nodes) {
        x.dump();
    }
    DEBUG_ASSERT_EQ(bt_thread_vars()->wr_locked_nodes.size(), 0);
    DEBUG_ASSERT_EQ(bt_thread_vars()->rd_locked_nodes.size(), 0);
}
#endif

} // namespace btree
} // namespace sisl
