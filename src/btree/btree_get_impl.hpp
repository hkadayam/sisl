#pragma once
#include "btree.hpp"

namespace sisl {
namespace btree {
template < typename K, typename V >
btree_status_t Btree< K, V >::do_get(const BtreeNodePtr< K >& my_node, BtreeGetRequest& greq) const {
    btree_status_t ret{btree_status_t::success};
    bool found;
    uint32_t idx;

    if (my_node->is_leaf()) {
        if (is_get_any_request(greq)) {
            auto& gareq = to_get_any_req(greq);
            std::tie(found, idx) =
                my_node->get_any(gareq.m_range, gareq.m_outkey.get(), gareq.m_outval.get(), true, true);
            if (found) { call_on_read_kv_cb(my_node, idx, gareq); }
        } else {
            auto& sgreq = to_single_get_req(greq);
            std::tie(found, idx) = my_node->find(sgreq.key(), sgreq.m_outval.get(), true);
            if (found) { call_on_read_kv_cb(my_node, idx, sgreq); }
        }
        if (!found) { ret = btree_status_t::not_found; }
        unlock_node(my_node, locktype_t::READ);
        return ret;
    }

    BtreeNodeInfo child_info;
    if (is_get_any_request(greq)) {
        std::tie(found, idx) = my_node->find(to_get_any_req(greq).m_range.start_key(), &child_info, true);
    } else {
        std::tie(found, idx) = my_node->find(to_single_get_req(greq).key(), &child_info, true);
    }

    ASSERT_IS_VALID_INTERIOR_CHILD_INDX(found, idx, my_node);
    BtreeNodePtr< K > child_node;
    ret = read_and_lock_node(child_info.bnode_id(), child_node, locktype_t::READ, locktype_t::READ, nullptr);
    if (ret != btree_status_t::success) { goto out; }

    unlock_node(my_node, locktype_t::READ);
    return (do_get(child_node, greq));

out:
    unlock_node(my_node, locktype_t::READ);
    return ret;
}
} // namespace btree
} // namespace sisl
