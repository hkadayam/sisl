#pragma once
#include "btree.hpp"

namespace sisl {
namespace btree {
template < typename K, typename V >
btree_status_t Btree< K, V >::do_remove(const BtreeNodePtr< K >& my_node, locktype_t curlock,
                                        BtreeRemoveRequest& rreq) {
    btree_status_t ret = btree_status_t::success;
    if (my_node->is_leaf()) {
        BT_NODE_DBG_ASSERT_EQ(curlock, locktype_t::WRITE, my_node);

        uint32_t removed_count{0};
        bool modified{false};
#ifndef NDEBUG
        my_node->validate_key_order();
#endif

        if (is_single_remove_request(rreq)) {
            auto& rsreq = to_single_remove_req(rreq);
            if ((modified = my_node->remove_one(rsreq.key(), nullptr, rsreq.m_outval.get()))) { ++removed_count; }
        } else if (is_range_remove_request(rreq)) {
            auto& rrreq = to_range_remove_req(rreq);
            if (rrreq.next_key().is_extent_key()) {
                modified = remove_extents_in_leaf(my_node, rrreq);
            } else {
                auto& subrange = rrreq.current_sub_range();
                auto const [start_found, start_idx] = my_node->find(subrange.start_key(), nullptr, false);
                auto [end_found, end_idx] = my_node->find(subrange.end_key(), nullptr, false);
                if (end_found) { ++end_idx; }
                for (auto idx{start_idx}; idx < end_idx; ++idx) {
                    call_on_remove_kv_cb(my_node, idx, rrreq);
                    my_node->remove(idx);
                    modified = true;
                }
                removed_count = end_idx - start_idx;
            }
        } else {
            auto& rareq = to_remove_any_req(rreq);
            if ((modified = my_node->remove_any(rareq.m_range, rareq.m_outkey.get(), rareq.m_outval.get()))) {
                ++removed_count;
            }
        }
#ifndef NDEBUG
        my_node->validate_key_order();
#endif
        if (modified) {
            write_node(my_node, nullptr, remove_req_op_ctx(rreq));
            COUNTER_DECREMENT(m_metrics, btree_obj_count, removed_count);
        }

        unlock_node(my_node, curlock);
        return modified ? btree_status_t::success : btree_status_t::not_found;
    }

retry:
    locktype_t child_cur_lock = locktype_t::NONE;
    bool found;
    uint32_t idx;

    // Get the childPtr for given key.
    BtreeNodeInfo child_info;
    if (is_remove_any_request(rreq)) {
        std::tie(found, idx) = my_node->find(to_remove_any_req(rreq).m_range.start_key(), &child_info, true);
    } else {
        std::tie(found, idx) = my_node->find(to_single_remove_req(rreq).key(), &child_info, true);
    }

    ASSERT_IS_VALID_INTERIOR_CHILD_INDX(found, idx, my_node);

    BtreeNodePtr< K > child_node;
    ret = get_child_and_lock_node(my_node, idx, child_info, child_node, locktype_t::READ, locktype_t::WRITE,
                                  remove_req_op_ctx(rreq));
    if (ret != btree_status_t::success) {
        unlock_node(my_node, curlock);
        return ret;
    }

    // Check if child node is minimal.
    child_cur_lock = child_node->is_leaf() ? locktype_t::WRITE : locktype_t::READ;
    if (child_node->is_merge_needed(m_bt_cfg)) {
        uint32_t node_end_idx = my_node->get_total_entries();
        if (!my_node->has_valid_edge()) { --node_end_idx; }
        uint32_t const end_idx = std::min((idx + m_bt_cfg.m_rebalance_max_nodes), node_end_idx);

        if (end_idx > idx) {
            // If we are unable to upgrade the node, ask the caller to retry.
            ret = upgrade_node_locks(my_node, child_node, child_cur_lock, remove_req_op_ctx(rreq));
            if (ret != btree_status_t::success) { return ret; }
            curlock = locktype_t::WRITE;

            auto result = merge_nodes(my_node, idx, end_idx, remove_req_op_ctx(rreq));
            if (result != btree_status_t::success && result != btree_status_t::merge_not_required) {
                unlock_node(child_node, locktype_t::WRITE);
                unlock_node(my_node, locktype_t::WRITE);
                return ret;
            }

            if (result == btree_status_t::success) {
                unlock_node(child_node, locktype_t::WRITE);
                child_cur_lock = locktype_t::NONE;
                COUNTER_INCREMENT(m_metrics, btree_merge_count, 1);
                goto retry;
            }
        }
    }

#ifndef NDEBUG
    if (idx != my_node->get_total_entries() && child_node->get_total_entries()) { // not edge
        BT_NODE_DBG_ASSERT_LE(child_node->get_last_key().compare(my_node->get_nth_key(idx, false)), 0, my_node);
    }

    if (idx > 0 && child_node->get_total_entries()) { // not first child
        BT_NODE_DBG_ASSERT_LT(child_node->get_first_key().compare(my_node->get_nth_key(idx - 1, false)), 0, my_node);
    }
#endif

    unlock_node(my_node, curlock);
    return (do_remove(child_node, child_cur_lock, rreq));

    // Warning: Do not access childNode or myNode beyond this point, since it would
    // have been unlocked by the recursive function and it could also been deleted.
}

template < typename K, typename V >
bool Btree< K, V >::remove_extents_in_leaf(const BtreeNodePtr< K >& node, BtreeRangeRemoveRequest& rrreq) {
    if constexpr (std::is_base_of_v< ExtentBtreeKey< K >, K > && std::is_base_of_v< ExtentBtreeValue< V >, V >) {
        const BtreeKeyRange& subrange = rrreq.current_sub_range();
        const auto& start_key = static_cast< const ExtentBtreeKey< K >& >(subrange.start_key());
        const auto& end_key = static_cast< ExtentBtreeKey< K >& >(subrange.end_key());

        auto const [start_found, start_idx] = node->find(start_key, nullptr, false);
        auto const [end_found, end_idx] = node->find(end_key, nullptr, false);

        K h_k, t_k;
        V h_v, t_v;
        int64_t head_offset{0};
        int64_t tail_offset{0};
        ExtentBtreeKey< K >& head_k = static_cast< ExtentBtreeKey< K >& >(h_k);
        ExtentBtreeKey< K >& tail_k = static_cast< ExtentBtreeKey< K >& >(t_k);
        ExtentBtreeValue< V >& head_v = static_cast< ExtentBtreeValue< V >& >(h_v);
        ExtentBtreeValue< V >& tail_v = static_cast< ExtentBtreeValue< V >& >(t_v);

        if (start_found) {
            head_k = node->get_nth_key(start_idx, false);
            head_offset = head_k.distance_start(start_key);
            BT_NODE_DBG_ASSERT_GE(head_offset, 0, node, "Invalid start_key or head_k");
            if (head_offset > 0) { node->get_nth_value(start_idx, &head_v, false); }
        }
        if (end_found) {
            tail_k = node->get_nth_key(end_idx, false);
            tail_offset = end_key.distance_end(tail_k);
            BT_NODE_DBG_ASSERT_GE(tail_offset, 0, node, "Invalid end_key or tail_k");
            if (tail_offset > 0) { node->get_nth_value(end_idx, &tail_v, false); }
        }

        // Write partial head and tail kv. At this point we are committing and we can't go back and not update
        // some of the extents.
        auto idx = start_idx;
        if (end_idx == start_idx) {
            // Special case - where there is a overlap and single entry is split into 3
            auto const tail_start = tail_k.extent_length() - tail_offset;
            if (m_on_remove_cb) {
                m_on_remove_cb(head_k.extract(head_offset, tail_start - head_offset, false),
                               head_v.extract(head_offset, tail_start - head_offset, false), rrreq);
            }

            if (tail_offset > 0) {
                node->insert(end_idx + 1, tail_k.extract(tail_start, tail_offset, false),
                             tail_v.extract(tail_start, tail_offset, false));
                COUNTER_INCREMENT(m_metrics, btree_obj_count, 1);
            }

            if (head_offset > 0) {
                node->update(idx++, head_k.extract(0, head_offset, false), head_v.extract(0, head_offset, false));
            }
        } else {
            if (tail_offset > 0) {
                auto const tail_start = tail_k.extent_length() - tail_offset;
                auto const shrunk_k = tail_k.extract(tail_start, tail_offset, false);
                call_on_update_kv_cb(node, end_idx, shrunk_k, rrreq);
                node->update(end_idx, shrunk_k, tail_v.extract(tail_start, tail_offset, false));
            } else if (end_found) {
                ++end_idx;
            }
            if (head_offset > 0) {
                auto const shrunk_k = head_k.extract(0, -head_offset, false);
                call_on_update_kv_cb(node, idx, shrunk_k, rrreq);
                node->update(idx++, shrunk_k, head_v.extract(0, -head_offset, false));
            }
        }

        // Remove everything in-between
        if (idx < end_idx) {
            if (m_on_remove_cb) {
                for (auto i{idx}; i <= end_idx; ++i) {
                    call_on_remove_kv_cb(node, i, rrreq);
                }
            }
            node->remove(idx, end_idx - 1);
            COUNTER_DECREMENT(m_metrics, btree_obj_count, end_idx - idx);
        }
        return true;
    } else {
        BT_REL_ASSERT(false, "Don't expect remove_extents to be called on non-extent code path");
        return false;
    }
}

template < typename K, typename V >
btree_status_t Btree< K, V >::merge_nodes(const BtreeNodePtr< K >& parent_node, uint32_t start_indx, uint32_t end_indx,
                                          void* context) {
    std::vector< BtreeNodePtr< K > > child_nodes;
    for (auto indx = start_indx; indx <= end_indx; ++indx) {
        if (indx == parent_node->get_total_entries()) {
            BT_NODE_LOG_ASSERT(parent_node->has_valid_edge(), parent_node,
                               "Assertion failure, expected valid edge for parent_node: {}");
        }

        BtreeNodeInfo child_info;
        parent_node->get_nth_value(indx, &child_info, false /* copy */);

        BtreeNodePtr< K > child;
        ret = read_and_lock_node(child_info.bnode_id(), child, locktype_t::WRITE, locktype_t::WRITE, context);
        if (ret != btree_status_t::success) { goto out; }
        BT_NODE_LOG_ASSERT_EQ(child->is_valid_node(), true, child);

        child_nodes.push_back(child);
    }

out:
    // Loop again in reverse order to unlock the nodes. freeable nodes need to be unlocked and freed
    for (uint32_t i = child_nodes.size() - 1; i != 0; i--) {
        unlock_node(child_nodes[i], locktype_t::WRITE);
    }
}

template < typename K, typename V >
btree_status_t Btree< K, V >::merge_nodes(const BtreeNodePtr< K >& parent_node, uint32_t start_indx, uint32_t end_indx,
                                          void* context) {
    btree_status_t ret = btree_status_t::merge_failed;
    std::vector< BtreeNodePtr< K > > child_nodes;
    std::vector< BtreeNodePtr< K > > old_nodes;
    std::vector< BtreeNodePtr< K > > replace_nodes;
    std::vector< BtreeNodePtr< K > > new_nodes;
    std::vector< BtreeNodePtr< K > > deleted_nodes;
    BtreeNodePtr< K > left_most_node;
    K last_pkey; // last key of parent node
    bool last_pkey_valid = false;
    uint32_t balanced_size;
    BtreeNodePtr< K > merge_node;
    K last_ckey; // last key in child
    uint32_t parent_insert_indx = start_indx;
#ifndef NDEBUG
    uint32_t total_child_entries = 0;
    uint32_t new_entries = 0;
    K last_debug_ckey;
    K new_last_debug_ckey;
    BtreeNodePtr< K > last_node;
#endif
    K child_pkey;

    /* Try to take a lock on all nodes participating in merge*/
    for (auto indx = start_indx; indx <= end_indx; ++indx) {
        if (indx == parent_node->get_total_entries()) {
            BT_NODE_LOG_ASSERT(parent_node->has_valid_edge(), parent_node,
                               "Assertion failure, expected valid edge for parent_node: {}");
        }

        BtreeNodeInfo child_info;
        parent_node->get_nth_value(indx, &child_info, false /* copy */);

        BtreeNodePtr< K > child;
        ret = read_and_lock_node(child_info.bnode_id(), child, locktype_t::WRITE, locktype_t::WRITE, context);
        if (ret != btree_status_t::success) { goto out; }
        BT_NODE_LOG_ASSERT_EQ(child->is_valid_node(), true, child);

        /* check if left most node has space */
        if (indx == start_indx) {
            balanced_size = m_bt_cfg.ideal_fill_size();
            left_most_node = child;
            if (left_most_node->get_occupied_size(m_bt_cfg) > balanced_size) {
                /* first node doesn't have any free space. we can exit now */
                ret = btree_status_t::merge_not_required;
                goto out;
            }
        } else {
            bool is_allocated = true;
            /* pre allocate the new nodes. We will free the nodes which are not in use later */
            auto new_node = alloc_node(child->is_leaf(), is_allocated, child);
            if (is_allocated) {
                /* we are going to allocate new blkid of all the nodes except the first node.
                 * Note :- These blkids will leak if we fail or crash before writing entry into
                 * journal.
                 */
                old_nodes.push_back(child);
                COUNTER_INCREMENT_IF_ELSE(m_metrics, child->is_leaf(), btree_leaf_node_count, btree_int_node_count, 1);
            }
            /* Blk IDs can leak if it crash before writing it to a journal */
            if (new_node == nullptr) {
                ret = btree_status_t::space_not_avail;
                goto out;
            }
            new_nodes.push_back(new_node);
        }
#ifndef NDEBUG
        total_child_entries += child->get_total_entries();
        last_debug_ckey = child->get_last_key();
#endif
        child_nodes.push_back(child);
    }

    if (end_indx != parent_node->get_total_entries()) {
        /* If it is not edge we always preserve the last key in a given merge group of nodes.*/
        parent_node->get_nth_key(end_indx, last_pkey, true);
        last_pkey_valid = true;
    }

    merge_node = left_most_node;
    /* We can not fail from this point. Nodes will be modified in memory. */
    for (uint32_t i = 0; i < new_nodes.size(); ++i) {
        auto occupied_size = merge_node->get_occupied_size(m_bt_cfg);
        if (occupied_size < balanced_size) {
            uint32_t pull_size = balanced_size - occupied_size;
            merge_node->move_in_from_right_by_size(m_bt_cfg, *(new_nodes[i]), pull_size);
            if (new_nodes[i]->get_total_entries() == 0) {
                /* this node is freed */
                deleted_nodes.push_back(new_nodes[i]);
                continue;
            }
        }

        /* update the last key of merge node in parent node */
        last_ckey = merge_node->get_last_key(); // last key in child
        BtreeNodeInfo ninfo(merge_node->get_node_id());
        parent_node->update(parent_insert_indx, last_ckey, ninfo);
        ++parent_insert_indx;

        merge_node->set_next_bnode(new_nodes[i]->get_node_id()); // link them
        merge_node = new_nodes[i];
        if (merge_node != left_most_node) {
            /* left most node is not replaced */
            replace_nodes.push_back(merge_node);
        }
    }

    /* update the latest merge node */
    last_ckey = merge_node->get_last_key();
    if (last_pkey_valid) {
        BT_NODE_DBG_ASSERT_LE(last_ckey.compare(last_pkey), 0, parent_node);
        last_ckey = last_pkey;
    }

    /* update the last key */
    {
        BtreeNodeInfo ninfo(merge_node->get_node_id());
        parent_node->update(parent_insert_indx, last_ckey, ninfo);
        ++parent_insert_indx;
    }

    /* remove the keys which are no longer used */
    if ((parent_insert_indx) <= end_indx) { parent_node->remove(parent_insert_indx, end_indx); }

    // TODO: Validate if empty child_pkey on last_key or edge has any impact on journal/precommit
    if (start_indx < parent_node->get_total_entries()) {
        child_pkey = parent_node->get_nth_key(start_indx, true);
        BT_NODE_REL_ASSERT_EQ(start_indx, (parent_insert_indx - 1), parent_node, "it should be last index");
    }

    merge_node_precommit(false, parent_node, parent_insert_indx, left_most_node, &old_nodes, &replace_nodes, context);

#if 0
    /* write the journal entry */
    if (BtreeStoreType == btree_store_type::SSD_BTREE) {
        auto j_iob = btree_store_t::make_journal_entry(journal_op::BTREE_MERGE, false /* is_root */, bcp,
                                                       {parent_node->get_node_id(), parent_node->get_gen()});
        K child_pkey;
        if (start_indx < parent_node->get_total_entries()) {
            parent_node->get_nth_key(start_indx, &child_pkey, true);
            BT_REL_ASSERT_CMP(start_indx, ==, (parent_insert_indx - 1), parent_node, "it should be last index");
        }
        btree_store_t::append_node_to_journal(j_iob, bt_journal_node_op::inplace_write, left_most_node, bcp,
                                              child_pkey.get_blob());
        for (auto& node : old_nodes) {
            btree_store_t::append_node_to_journal(j_iob, bt_journal_node_op::removal, node, bcp);
        }
        uint32_t insert_indx = 0;
        for (auto& node : replace_nodes) {
            K child_pkey;
            if ((start_indx + insert_indx) < parent_node->get_total_entries()) {
                parent_node->get_nth_key(start_indx + insert_indx, &child_pkey, true);
                BT_REL_ASSERT_CMP((start_indx + insert_indx), ==, (parent_insert_indx - 1), parent_node,
                                      "it should be last index");
            }
            btree_store_t::append_node_to_journal(j_iob, bt_journal_node_op::creation, node, bcp,
                                                  child_pkey.get_blob());
            ++insert_indx;
        }
        BT_REL_ASSERT_CMP((start_indx + insert_indx), ==, parent_insert_indx, parent_node, "it should be same");
        btree_store_t::write_journal_entry(m_btree_store.get(), bcp, j_iob);
    }
#endif

    if (replace_nodes.size() > 0) {
        /* write the right most node */
        write_node(replace_nodes[replace_nodes.size() - 1], nullptr, context);
        if (replace_nodes.size() > 1) {
            /* write the middle nodes */
            for (int i = replace_nodes.size() - 2; i >= 0; --i) {
                write_node(replace_nodes[i], replace_nodes[i + 1], context);
            }
        }
        /* write the left most node */
        write_node(left_most_node, replace_nodes[0], context);
    } else {
        /* write the left most node */
        write_node(left_most_node, nullptr, context);
    }

    /* write the parent node */
    write_node(parent_node, left_most_node, context);

#ifndef NDEBUG
    for (const auto& n : replace_nodes) {
        new_entries += n->get_total_entries();
    }

    new_entries += left_most_node->get_total_entries();
    BT_DBG_ASSERT_EQ(total_child_entries, new_entries);

    if (replace_nodes.size()) {
        replace_nodes[replace_nodes.size() - 1]->get_last_key(&new_last_debug_ckey);
        last_node = replace_nodes[replace_nodes.size() - 1];
    } else {
        new_last_debug_ckey = left_most_node->get_last_key();
        last_node = left_most_node;
    }
    if (last_debug_ckey.compare(&new_last_debug_ckey) != 0) {
        LOGINFO("{}", last_node->to_string());
        if (deleted_nodes.size() > 0) { LOGINFO("{}", (deleted_nodes[deleted_nodes.size() - 1]->to_string())); }
        BT_DBG_ASSERT(false, "compared failed");
    }
#endif
    /* free nodes. It actually gets freed after cp is completed */
    for (const auto& n : old_nodes) {
        free_node(n, locktype_t::WRITE, context);
    }
    for (const auto& n : deleted_nodes) {
        free_node(n, locktype_t::WRITE, context);
    }
    ret = btree_status_t::success;
out:
#ifndef NDEBUG
    uint32_t freed_entries = deleted_nodes.size();
    uint32_t scan_entries = end_indx - start_indx - freed_entries + 1;
    for (uint32_t i = 0; i < scan_entries; ++i) {
        if (i < (scan_entries - 1)) { validate_sanity_next_child(parent_node, (uint32_t)start_indx + i); }
        validate_sanity_child(parent_node, (uint32_t)start_indx + i);
    }
#endif
    // Loop again in reverse order to unlock the nodes. freeable nodes need to be unlocked and freed
    for (uint32_t i = child_nodes.size() - 1; i != 0; i--) {
        unlock_node(child_nodes[i], locktype_t::WRITE);
    }
    unlock_node(child_nodes[0], locktype_t::WRITE);
    if (ret != btree_status_t::success) {
        /* free the allocated nodes */
        for (const auto& n : new_nodes) {
            free_node(n, locktype_t::WRITE, context);
        }
    }
    return ret;
}
} // namespace btree
} // namespace sisl
