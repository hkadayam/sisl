#pragma once
#include "btree.hpp"

namespace sisl {
namespace btree {

/* This function does the heavy lifiting of co-ordinating inserts. It is a recursive function which walks
 * down the tree.
 *
 * NOTE: It expects the node it operates to be locked (either read or write) and also the node should not be
 * full.
 *
 * Input:
 * myNode      = Node it operates on
 * curLock     = Type of lock held for this node
 * req         = Req with information of current key/value to insert
 */
template < typename K, typename V >
template < typename ReqT >
btree_status_t Btree< K, V >::do_put(const BtreeNodePtr< K >& my_node, locktype_t curlock, ReqT& req) {
    btree_status_t ret = btree_status_t::success;

    if (my_node->is_leaf()) {
        /* update the leaf node */
        BT_NODE_LOG_ASSERT_EQ(curlock, locktype_t::WRITE, my_node);
        ret = mutate_write_leaf_node(my_node, req);
        unlock_node(my_node, curlock);
        return ret;
    }

retry:
    uint32_t start_ind{0};
    uint32_t end_ind{0};
    uint32_t curr_ind;

    if constexpr (std::is_same_v< ReqT, BtreeRangePutRequest >) {
        const auto count = my_node->template get_all< V >(req.next_range(), UINT32_MAX, start_ind, end_ind);
        if (count == 0) {
            BT_NODE_LOG_ASSERT(false, my_node, "get_all returns 0 entries for interior node is not valid pattern");
            ret = btree_status_t::retry;
            goto out;
        }
    } else if constexpr (std::is_same_v< ReqT, BtreeSinglePutRequest >) {
        auto const [found, idx] = my_node->find(req.key(), nullptr, true);
        ASSERT_IS_VALID_INTERIOR_CHILD_INDX(found, idx, my_node);
        end_ind = start_ind = idx;
    }

    BT_NODE_DBG_ASSERT((curlock == locktype_t::READ || curlock == locktype_t::WRITE), my_node, "unexpected locktype {}",
                       curlock);

    curr_ind = start_ind;
    while (curr_ind <= end_ind) { // iterate all matched childrens
#if 0
#ifdef _PRERELEASE
        if (curr_ind - start_ind > 1 && homestore_flip->test_flip("btree_leaf_node_split")) {
            ret = btree_status_t::retry;
            goto out;
        }
#endif
#endif
        locktype_t child_cur_lock = locktype_t::NONE;

        // Get the childPtr for given key.
        BtreeNodeInfo child_info;
        BtreeNodePtr< K > child_node;
        ret = get_child_and_lock_node(my_node, curr_ind, child_info, child_node, locktype_t::READ, locktype_t::WRITE,
                                      req.m_op_context);
        if (ret != btree_status_t::success) {
            if (ret == btree_status_t::not_found) {
                // Either the node was updated or mynode is freed. Just proceed again from top.
                /* XXX: Is this case really possible as we always take the parent lock and never
                 * release it.
                 */
                ret = btree_status_t::retry;
            }
            goto out;
        }

        // Directly get write lock for leaf, since its an insert.
        child_cur_lock = (child_node->is_leaf()) ? locktype_t::WRITE : locktype_t::READ;

        if (is_split_needed(child_node, m_bt_cfg, req)) {
            if (curlock != locktype_t::WRITE) {
                ret = upgrade_node_locks(my_node, child_node, child_cur_lock, req.m_op_context);
                if (ret != btree_status_t::success) {
                    BT_NODE_LOG(DEBUG, my_node, "Upgrade of node lock failed, retrying from root");
                    curlock = locktype_t::NONE; // upgrade_node_lock releases all locks on failure
                    child_cur_lock = locktype_t::NONE;
                    goto out;
                }
                curlock = locktype_t::WRITE;
            }

            K split_key;
            ret = split_node(my_node, child_node, curr_ind, &split_key, false /* root_split */, req.m_op_context);
            unlock_node(child_node, locktype_t::WRITE);
            child_cur_lock = locktype_t::NONE;

            if (ret != btree_status_t::success) {
                goto out;
            } else {
                COUNTER_INCREMENT(m_metrics, btree_split_count, 1);
                goto retry; // After split, retry search and walk down.
            }
        }

        /* Get subrange if it is a range update */
        if constexpr (std::is_same_v< ReqT, BtreeRangePutRequest >) {
            if (child_node->is_leaf()) {
                // We get the subrange only for leaf because this is where we will be inserting keys. In interior
                // nodes, keys are always propogated from the lower nodes.
                BtreeSearchState& search_state = req.search_state();
                const auto& inp_range = search_state.input_range();
                auto& cur_range = s_cast< BtreeKeyRangeSafe< K >& >(search_state.mutable_sub_range());
                bool is_inp_key_lesser;
                cur_range.m_actual_end_key =
                    my_node->min_of(s_cast< const K& >(inp_range.end_key()), curr_ind, is_inp_key_lesser);
                cur_range.m_end_incl = is_inp_key_lesser ? search_state.input_range().is_end_inclusive() : true;

                BT_NODE_LOG(DEBUG, my_node, "Subrange:s:{},e:{},c:{},nid:{},edgeid:{},sk:{},ek:{}", start_ind, end_ind,
                            curr_ind, my_node->get_node_id(), my_node->get_edge_id(),
                            search_state.current_sub_range().start_key().to_string(),
                            search_state.current_sub_range().end_key().to_string());
            }
        }

#ifndef NDEBUG
        K ckey, pkey;
        if (curr_ind != my_node->total_entries()) { // not edge
            pkey = my_node->get_nth_key(curr_ind, true);
            if (child_node->total_entries() != 0) {
                ckey = child_node->get_last_key();
                if (!child_node->is_leaf()) {
                    BT_NODE_DBG_ASSERT_EQ(ckey.compare(pkey), 0, my_node);
                } else {
                    BT_NODE_DBG_ASSERT_LE(ckey.compare(pkey), 0, my_node);
                }
            }
            // BT_NODE_DBG_ASSERT_EQ((is_range_put_req(req) || k.compare(pkey) <= 0), true, child_node);
        }
        if (curr_ind > 0) { // not first child
            pkey = my_node->get_nth_key(curr_ind - 1, true);
            if (child_node->total_entries() != 0) {
                ckey = child_node->get_first_key();
                BT_NODE_DBG_ASSERT_GE(ckey.compare(pkey), 0, child_node);
            }
            // BT_NODE_DBG_ASSERT_EQ((is_range_put_req(req) || k.compare(pkey) >= 0), true, my_node);
        }
#endif
        if (curr_ind == end_ind) {
            // If we have reached the last index, unlock before traversing down, because we no longer need
            // this lock. Holding this lock will impact performance unncessarily.
            unlock_node(my_node, curlock);
            curlock = locktype_t::NONE;
        }

        ret = do_put(child_node, child_cur_lock, req);
        if (ret != btree_status_t::success) { goto out; }

        ++curr_ind;
    }
out:
    if (curlock != locktype_t::NONE) { unlock_node(my_node, curlock); }
    return ret;
    // Warning: Do not access childNode or myNode beyond this point, since it would
    // have been unlocked by the recursive function and it could also been deleted.
}

template < typename K, typename V >
template < typename ReqT >
btree_status_t Btree< K, V >::mutate_write_leaf_node(const BtreeNodePtr< K >& my_node, ReqT& req) {
    btree_status_t ret = btree_status_t::success;
    if constexpr (std::is_same_v< ReqT, BtreeRangePutRequest >) {
        BtreeSearchState& search_state = req.search_state();
        const BtreeKeyRange& subrange = search_state.current_sub_range();

        if (subrange.start_key().is_extent_key()) {
            ret = mutate_extents_in_leaf(my_node, req);
        } else {
            auto const [start_found, start_idx] = my_node->find(subrange.start_key(), nullptr, false);
            auto const [end_found, end_idx] = my_node->find(subrange.end_key(), nullptr, false);
            if (req.m_put_type != btree_put_type::REPLACE_ONLY_IF_EXISTS) {
                BT_DBG_ASSERT(false, "For non-extent keys range-update should be really update and cannot insert");
                ret = btree_status_t::not_supported;
            } else {
                for (auto idx{start_idx}; idx <= end_idx; ++idx) {
                    my_node->update(idx, *req.m_newval);
                }
            }
            // update cursor in intermediate search state
            search_state.set_cursor_key< K >(subrange.end_key());
        }
    } else if constexpr (std::is_same_v< ReqT, BtreeSinglePutRequest >) {
        if (!my_node->put(req.key(), req.value(), req.m_put_type, req.m_existing_val.get())) {
            ret = btree_status_t::put_failed;
        }
        COUNTER_INCREMENT(m_metrics, btree_obj_count, 1);
    }

    if ((ret == btree_status_t::success) || (ret == btree_status_t::has_more)) {
        write_node(my_node, nullptr, req.m_op_context);
    }
    return ret;
}

template < typename K, typename V >
btree_status_t Btree< K, V >::mutate_extents_in_leaf(const BtreeNodePtr< K >& node, BtreeRangePutRequest& rpreq) {
    if constexpr (std::is_base_of_v< ExtentBtreeKey< K >, K > && std::is_base_of_v< ExtentBtreeValue< V >, V >) {
        const BtreeKeyRange& subrange = rpreq.current_sub_range();
        const auto& start_key = static_cast< const ExtentBtreeKey< K >& >(subrange.start_key());
        const auto& end_key = static_cast< ExtentBtreeKey< K >& >(subrange.end_key());
        ExtentBtreeValue< V >* new_value = static_cast< ExtentBtreeValue< V >* >(rpreq.m_newval.get());
        btree_status_t ret{btree_status_t::success};

        BT_DBG_ASSERT_EQ(start_key.extent_length(), 1, "Search range start key can't be multiple extents");
        BT_DBG_ASSERT_EQ(end_key.extent_length(), 1, "Search range end key can't be multiple extents");

        if (!can_extents_auto_merge()) {
            BT_REL_ASSERT(false, "Yet to support non-auto merge range of extents in range put");
            return btree_status_t::not_supported;
        }

        bool retry{false};
        auto const [start_found, start_idx] = node->find(start_key, nullptr, false);
        do {
            auto const [end_found, end_idx] = node->find(end_key, nullptr, false);
            ExtentBtreeKey const new_k = start_key.combine(end_key);
            auto idx = start_idx;

            { // Scope this to avoid head_k and tail_k are used beyond
                K h_k, t_k;
                V h_v, t_v;
                int64_t head_offset{0};
                int64_t tail_offset{0};
                ExtentBtreeKey< K >& head_k = static_cast< ExtentBtreeKey< K >& >(h_k);
                ExtentBtreeKey< K >& tail_k = static_cast< ExtentBtreeKey< K >& >(t_k);
                ExtentBtreeValue< V >& head_v = static_cast< ExtentBtreeValue< V >& >(h_v);
                ExtentBtreeValue< V >& tail_v = static_cast< ExtentBtreeValue< V >& >(t_v);

                // Get the residue head and tail key first if it is present, before updating any fields, otherwise
                // updating fields will modify the other entry.
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

                // Shortcut to simple update of the existing range, which is a normal case. Its a simple update only if
                // the value we are replacing is all equal sized for every extent piece (which is normal use cases of
                // the extents)
                if (start_found && end_found && (head_offset == 0) && (tail_offset == 0) && (start_idx == end_idx) &&
                    new_value->is_equal_sized()) {
                    call_on_update_kv_cb(node, start_idx, new_k, rpreq);
                    node->update(start_idx, new_k, new_value->shift(new_k.extent_length(), false));
                    break;
                }

                // Do size check, first check if we can accomodate the keys if checked conservatively. Thats most common
                // case and thus efficient. Next we go aggressively, the more aggressive the check, more performance
                // impact.
                //
                // First level check: Try assuming the entire value + 2 keys + 2 records to be inserted. If there is a
                // space available, no need any additional check.
                auto const record_size = (2 * (new_k.serialized_size() + node->get_record_size()));
                auto size_needed = new_value->extracted_size(0, new_k.extent_length()) + record_size;

                auto const available_space = node->get_available_size(m_bt_cfg);
                if (size_needed > available_space) {
                    BT_NODE_DBG_ASSERT_EQ(retry, false, node, "Don't expect multiple attempts of size not available");

                    // Second level check: Take into account the head and tail overlapped space and see if it saves some
                    if (head_offset > 0) {
                        size_needed -= (head_v.serialized_size() - head_v.extracted_size(0, head_offset));
                    }
                    if (tail_offset > 0) { size_needed -= tail_v.extracted_size(0, tail_offset); }

                    if (size_needed > available_space) {
                        // Third level check: Walk through every entry in the about to remove list and account for
                        // theirs
                        V tmp_v;
                        for (auto i = start_idx; i < end_idx; ++i) {
                            node->get_nth_value(i, &tmp_v, false);
                            size_needed -= (node->get_nth_key(i, false).serialized_size() + tmp_v.serialized_size());
                        }

                        // If still size is not enough, no other option other than trimming down the keys and retry
                        if (size_needed > available_space) {
                            auto const nextents = new_value->num_extents_fit(available_space - record_size);
                            end_key = new_k.extract(0, nextents, true);
                            retry = true;
                            ret = btree_status_t::has_more;
                            continue;
                        }
                    }
                }
                retry = false;

                // Write partial head and tail kv. At this point we are committing and we can't go back and not update
                // some of the extents.
                if (end_idx == start_idx) {
                    // Special case - where there is a overlap and single entry is split into 3
                    auto const tail_start = tail_k.extent_length() - tail_offset;
                    if (m_on_remove_cb) {
                        m_on_remove_cb(head_k.extract(head_offset, tail_start - head_offset, false),
                                       head_v.extract(head_offset, tail_start - head_offset, false), rpreq);
                    }

                    if (tail_offset > 0) {
                        node->insert(end_idx + 1, tail_k.extract(tail_start, tail_offset, false),
                                     tail_v.extract(tail_start, tail_offset, false));
                        COUNTER_INCREMENT(m_metrics, btree_obj_count, 1);
                    }

                    if (head_offset > 0) {
                        node->update(idx++, head_k.extract(0, head_offset, false),
                                     head_v.extract(0, head_offset, false));
                    }
                } else {
                    if (tail_offset > 0) {
                        auto const tail_start = tail_k.extent_length() - tail_offset;
                        auto const shrunk_k = tail_k.extract(tail_start, tail_offset, false);
                        call_on_update_kv_cb(node, end_idx, shrunk_k, rpreq);
                        node->update(end_idx, shrunk_k, tail_v.extract(tail_start, tail_offset, false));
                    } else if (end_found) {
                        ++end_idx;
                    }

                    if (head_offset > 0) {
                        auto const shrunk_k = head_k.extract(0, -head_offset, false);
                        call_on_update_kv_cb(node, idx, shrunk_k, rpreq);
                        node->update(idx++, shrunk_k, head_v.extract(0, -head_offset, false));
                    }
                }
            }

            // Remove everything in-between
            if (idx < end_idx) {
                if (m_on_remove_cb) {
                    for (auto i{idx}; i <= end_idx; ++i) {
                        call_on_remove_kv_cb(node, i, rpreq);
                    }
                }
                node->remove(idx, end_idx - 1);
                COUNTER_DECREMENT(m_metrics, btree_obj_count, end_idx - idx);
            }

            // Now we should have enough room to insert the combined entry
            node->insert(idx, new_k, new_value->shift(new_k.extent_length()));
            COUNTER_INCREMENT(m_metrics, btree_obj_count, 1);
        } while (retry);

        rpreq.search_state().set_cursor_key< K >(end_key);
        return ret;
    } else {
        BT_REL_ASSERT(false, "Don't expect mutate_extents to be called on non-extent code path");
        return btree_status_t::not_supported;
    }
}

template < typename K, typename V >
template < typename ReqT >
btree_status_t Btree< K, V >::check_split_root(ReqT& req) {
    K split_key;
    BtreeNodePtr< K > child_node = nullptr;
    btree_status_t ret = btree_status_t::success;
    BtreeNodePtr< K > root;
    BtreeNodePtr< K > new_root;

    m_btree_lock.lock();
    ret = read_and_lock_node(m_root_node_id, root, locktype_t::WRITE, locktype_t::WRITE, req.m_op_context);
    if (ret != btree_status_t::success) { goto done; }

    if (!is_split_needed(root, m_bt_cfg, req)) {
        unlock_node(root, locktype_t::WRITE);
        goto done;
    }

    new_root = alloc_interior_node();
    if (new_root == nullptr) {
        ret = btree_status_t::space_not_avail;
        unlock_node(root, locktype_t::WRITE);
        goto done;
    }

    BT_NODE_LOG(DEBUG, root, "Root node is full, creating new root node", new_root->get_node_id());
    new_root->set_edge_value(BtreeNodeInfo{root->get_node_id()});
    child_node = std::move(root);
    root = std::move(new_root);
    BT_NODE_DBG_ASSERT_EQ(root->total_entries(), 0, root);

    ret = split_node(root, child_node, root->total_entries(), &split_key, true, req.m_op_context);
    if (ret != btree_status_t::success) {
        free_node(root, locktype_t::WRITE, req.m_op_context);
        root = std::move(child_node);
        unlock_node(root, locktype_t::WRITE);
    } else {
        m_root_node_id = root->get_node_id();

        // TODO: Precommit the new root node id for persistent btree to recover from
        unlock_node(child_node, locktype_t::WRITE);
        COUNTER_INCREMENT(m_metrics, btree_depth, 1);
    }

done:
    m_btree_lock.unlock();
    return ret;
}

template < typename K, typename V >
btree_status_t Btree< K, V >::split_node(const BtreeNodePtr< K >& parent_node, const BtreeNodePtr< K >& child_node,
                                         uint32_t parent_ind, BtreeKey* out_split_key, bool root_split, void* context) {
    BtreeNodeInfo ninfo;
    BtreeNodePtr< K > child_node1 = child_node;
    BtreeNodePtr< K > child_node2 = child_node1->is_leaf() ? alloc_leaf_node() : alloc_interior_node();

    if (child_node2 == nullptr) { return (btree_status_t::space_not_avail); }

    btree_status_t ret = btree_status_t::success;

    child_node2->set_next_bnode(child_node1->next_bnode());
    child_node1->set_next_bnode(child_node2->get_node_id());
    uint32_t child1_filled_size = BtreeNode< K >::node_area_size(m_bt_cfg) - child_node1->get_available_size(m_bt_cfg);

    auto split_size = m_bt_cfg.split_size(child1_filled_size);
    uint32_t res = child_node1->move_out_to_right_by_size(m_bt_cfg, *child_node2, split_size);

    BT_NODE_REL_ASSERT_GT(res, 0, child_node1,
                          "Unable to split entries in the child node"); // means cannot split entries
    BT_NODE_DBG_ASSERT_GT(child_node1->total_entries(), 0, child_node1);

    // In an unlikely case where parent node has no room to accomodate the child key, we need to un-split and then
    // free up the new node. This situation could happen on variable key, where the key max size is purely
    // an estimation. This logic allows the max size to be declared more optimistically than say 1/4 of node
    // which will have substatinally large number of splits and performance constraints.
    if (out_split_key->serialized_size() > parent_node->get_available_size(m_bt_cfg)) {
        uint32_t move_in_res = child_node1->copy_by_entries(m_bt_cfg, *child_node2, 0, child_node2->total_entries());
        BT_NODE_REL_ASSERT_EQ(move_in_res, res, child_node1,
                              "The split key size is more than estimated parent available space, but when revert is "
                              "attempted it fails. Continuing can cause data loss, so crashing");
        free_node(child_node2, locktype_t::WRITE, context);

        // Mark the parent_node itself to be split upon next retry.
        bt_thread_vars()->force_split_node = parent_node;
        return btree_status_t::retry;
    }

    // Update the existing parent node entry to point to second child ptr.
    bool edge_split = (parent_ind == parent_node->total_entries());
    ninfo.set_bnode_id(child_node2->get_node_id());
    parent_node->update(parent_ind, ninfo);

    // Insert the last entry in first child to parent node
    *out_split_key = child_node1->get_last_key();
    ninfo.set_bnode_id(child_node1->get_node_id());

    // If key is extent then we always insert the tail portion of the extent key in the parent node
    if (out_split_key->is_extent_key()) {
        parent_node->insert(((ExtentBtreeKey< K >*)out_split_key)->extract_end(false), ninfo);
    } else {
        parent_node->insert(*out_split_key, ninfo);
    }

    BT_NODE_DBG_ASSERT_GT(child_node2->get_first_key().compare(*out_split_key), 0, child_node2);
    BT_NODE_LOG(DEBUG, parent_node, "Split child_node={} with new_child_node={}, split_key={}",
                child_node1->get_node_id(), child_node2->get_node_id(), out_split_key->to_string());

    split_node_precommit(parent_node, child_node1, child_node2, root_split, edge_split, context);

#if 0
    if (BtreeStoreType == btree_store_type::SSD_BTREE) {
        auto j_iob = btree_store_t::make_journal_entry(journal_op::BTREE_SPLIT, root_split, bcp,
                                                       {parent_node->get_node_id(), parent_node->get_gen()});
        btree_store_t::append_node_to_journal(
            j_iob, (root_split ? bt_journal_node_op::creation : bt_journal_node_op::inplace_write), child_node1, bcp,
            out_split_end_key.get_blob());

        // For root split or split around the edge, we don't write the key, which will cause replay to insert
        // edge
        if (edge_split) {
            btree_store_t::append_node_to_journal(j_iob, bt_journal_node_op::creation, child_node2, bcp);
        } else {
            K child2_pkey;
            parent_node->get_nth_key(parent_ind, &child2_pkey, true);
            btree_store_t::append_node_to_journal(j_iob, bt_journal_node_op::creation, child_node2, bcp,
                                                  child2_pkey.get_blob());
        }
        btree_store_t::write_journal_entry(m_btree_store.get(), bcp, j_iob);
    }
#endif

    // we write right child node, than left and than parent child
    write_node(child_node2, nullptr, context);
    write_node(child_node1, child_node2, context);
    write_node(parent_node, child_node1, context);

    // NOTE: Do not access parentInd after insert, since insert would have
    // shifted parentNode to the right.
    return ret;
}

template < typename K, typename V >
template < typename ReqT >
bool Btree< K, V >::is_split_needed(const BtreeNodePtr< K >& node, const BtreeConfig& cfg, ReqT& req) const {
    if (bt_thread_vars()->force_split_node && (bt_thread_vars()->force_split_node == node)) {
        bt_thread_vars()->force_split_node = nullptr;
        return true;
    }

    int64_t size_needed = 0;
    if (!node->is_leaf()) { // if internal node, size is atmost one additional entry, size of K/V
        size_needed = K::get_estimate_max_size() + BtreeNodeInfo::get_fixed_size() + node->get_record_size();
    } else if constexpr (std::is_same_v< ReqT, BtreeRangePutRequest >) {
        BtreeSearchState& search_state = req.search_state();
        const BtreeKey& next_key = search_state.next_key();

        if (next_key.is_extent_key()) {
            // For extent keys we expect to write atleast first value in the req along with 2 possible keys
            // in case of splitting existing key
            auto val = static_cast< const ExtentBtreeValue< V >* >(req.m_newval.get());
            size_needed = val->extracted_size(0, 1) + 2 * (next_key.serialized_size() + node->get_record_size());
        } else {
            size_needed = req.m_newval->serialized_size();
            if (req.m_put_type != btree_put_type::REPLACE_ONLY_IF_EXISTS) {
                size_needed += next_key.serialized_size() + node->get_record_size();
            }
        }
    } else if constexpr (std::is_same_v< ReqT, BtreeSinglePutRequest >) {
        size_needed = req.key().serialized_size() + req.value().serialized_size() + node->get_record_size();
    }
    int64_t alreadyFilledSize = BtreeNode< K >::node_area_size(cfg) - node->get_available_size(cfg);
    return (alreadyFilledSize + size_needed >= BtreeNode< K >::ideal_fill_size(cfg));
}

#if 0
template < typename K, typename V >
int64_t Btree< K, V >::compute_single_put_needed_size(const V& current_val, const V& new_val) const {
    return new_val.serialized_size() - current_val.serialized_size();
}

template < typename K, typename V >
int64_t Btree< K, V >::compute_range_put_needed_size(const std::vector< std::pair< K, V > >& existing_kvs,
                                                     const V& new_val) const {
    return new_val.serialized_size() * existing_kvs.size();
}

template < typename K, typename V >
btree_status_t
Btree< K, V >::custom_kv_select_for_write(uint8_t node_version, const std::vector< std::pair< K, V > >& match_kv,
                                          std::vector< std::pair< K, V > >& replace_kv, const BtreeKeyRange& range,
                                          const BtreeRangePutRequest& rpreq) const {
    for (const auto& [k, v] : match_kv) {
        replace_kv.push_back(std::make_pair(k, (V&)rpreq.m_newval));
    }
    return btree_status_t::success;
}
#endif

#if 0
template < typename K, typename V >
btree_status_t Btree< K, V >::get_start_and_end_ind(const BtreeNodePtr< K >& node, BtreeMutateRequest& req,
                                                    int& start_ind, int& end_ind) {
    btree_status_t ret = btree_status_t::success;
    if (is_range_put_req(req)) {
        /* just get start/end index from get_all. We don't release the parent lock until this
         * key range is not inserted from start_ind to end_ind.
         */
        node->template get_all< V >(to_range_put_req(req).next_range(), UINT32_MAX, (uint32_t&)start_ind,
                                    (uint32_t&)end_ind);
    } else {
        auto [found, idx] = node->find(to_single_put_req(req).key(), nullptr, true);
        ASSERT_IS_VALID_INTERIOR_CHILD_INDX(found, idx, node);
        end_ind = start_ind = (int)idx;
    }

    if (start_ind > end_ind) {
        BT_NODE_LOG_ASSERT(false, node, "start ind {} greater than end ind {}", start_ind, end_ind);
        ret = btree_status_t::retry;
    }
    return ret;
}
#endif

} // namespace btree
} // namespace sisl
