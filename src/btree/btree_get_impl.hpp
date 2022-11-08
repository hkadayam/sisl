#pragma once
#include "btree.hpp"

namespace sisl {
namespace btree {
template < typename K, typename V >
template < typename ReqT >
btree_status_t Btree< K, V >::do_get(const BtreeNodePtr< K >& my_node, ReqT& greq) const {
    btree_status_t ret{btree_status_t::success};
    bool found{false};
    uint32_t idx;

    if (my_node->is_leaf()) {
        if constexpr (std::is_same_v< BtreeGetAnyRequest< K >, ReqT >) {
            std::tie(found, idx) = my_node->get_any(greq.m_range, greq.m_outkey.get(), greq.m_outval.get(), true, true);
            if (found) { call_on_read_kv_cb(my_node, idx, greq); }
        } else if constexpr (std::is_same_v< BtreeSingleGetRequest, ReqT >) {
            std::tie(found, idx) = my_node->find(greq.key(), greq.m_outval.get(), true);
            if (found) { call_on_read_kv_cb(my_node, idx, greq); }
        }
        if (!found) { ret = btree_status_t::not_found; }
        unlock_node(my_node, locktype_t::READ);
        return ret;
    }

    BtreeNodeInfo child_info;
    if constexpr (std::is_same_v< BtreeGetAnyRequest< K >, ReqT >) {
        std::tie(found, idx) = my_node->find(greq.m_range.start_key(), &child_info, true);
    } else if constexpr (std::is_same_v< BtreeSingleGetRequest, ReqT >) {
        std::tie(found, idx) = my_node->find(greq.key(), &child_info, true);
    }

    ASSERT_IS_VALID_INTERIOR_CHILD_INDX(found, idx, my_node);
    BtreeNodePtr< K > child_node;
    ret = read_and_lock_node(child_info.bnode_id(), child_node, locktype_t::READ, locktype_t::READ, greq.m_op_context);
    if (ret != btree_status_t::success) { goto out; }

    unlock_node(my_node, locktype_t::READ);
    return (do_get(child_node, greq));

out:
    unlock_node(my_node, locktype_t::READ);
    return ret;
}
} // namespace btree
} // namespace sisl
