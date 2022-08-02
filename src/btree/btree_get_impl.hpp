#pragma once
#include "btree.hpp"

namespace sisl {
namespace btree {
template < typename K, typename V >
btree_status_t Btree< K, V >::do_get(const BtreeNodePtr& my_node, BtreeGetRequest& greq) const {
    btree_status_t ret = btree_status_t::success;
    bool is_child_lock = false;
    locktype_t child_locktype;

    if (my_node->is_leaf()) {
        const auto [found, idx] = my_node->find(greq.m_keys, greq.m_outkey, greq.m_outval);
        ret = found ? btree_status_t::success : btree_status_t::not_found;
        unlock_node(my_node, locktype_t::READ);
        return ret;
    }

    BtreeNodeInfo child_info;
    auto [found, idx] = my_node->find(greq.m_keys, nullptr, &child_info);
    ASSERT_IS_VALID_INTERIOR_CHILD_INDX(found, idx, my_node);

    BtreeNodePtr child_node;
    child_locktype = locktype_t::READ;
    ret = read_and_lock_child(child_info.bnode_id(), child_node, my_node, idx, child_locktype, child_locktype, nullptr);
    if (ret != btree_status_t::success) { goto out; }

    unlock_node(my_node, locktype_t::READ);
    return (do_get(child_node, greq));

out:
    unlock_node(my_node, locktype_t::READ);
    return ret;
}
} // namespace btree
} // namespace sisl
