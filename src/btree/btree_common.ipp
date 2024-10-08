#pragma once
#include "btree.hpp"

namespace sisl {
namespace btree {

template < typename K, typename V >
btree_status_t Btree< K, V >::post_order_traversal(locktype_t ltype, const auto& cb) {
    BtreeNodePtr< K > root;

    if (ltype == locktype_t::READ) {
        m_btree_lock.lock_shared();
    } else if (ltype == locktype_t::WRITE) {
        m_btree_lock.lock();
    }

    btree_status_t ret{btree_status_t::success};
    if (m_root_node_id != empty_bnodeid) {
        read_and_lock_root(m_root_node_id, root, ltype, ltype, nullptr);
        if (ret != btree_status_t::success) { goto done; }

        ret = post_order_traversal(root, ltype, cb);
    }
done:
    if (ltype == locktype_t::READ) {
        m_btree_lock.unlock_shared();
    } else if (ltype == locktype_t::WRITE) {
        m_btree_lock.unlock();
    }
    return ret;
}

template < typename K, typename V >
btree_status_t Btree< K, V >::post_order_traversal(const BtreeNodePtr< K >& node, locktype_t ltype, const auto& cb) {
    uint32_t i{0};
    btree_status_t ret = btree_status_t::success;

    if (!node->is_leaf()) {
        BtreeNodeInfo child_info;
        while (i <= node->get_total_entries()) {
            if (i == node->get_total_entries()) {
                if (!node->has_valid_edge()) { break; }
                child_info.set_bnode_id(node->get_edge_id());
            } else {
                node->get_nth_value(i, &child_info, false /* copy */);
            }

            BtreeNodePtr< K > child;
            ret = read_and_lock_child(child_info.bnode_id(), child, node, i, ltype, ltype, nullptr);
            if (ret != btree_status_t::success) { return ret; }
            ret = post_order_traversal(child, ltype, cb);
            unlock_node(child, ltype);
            ++i;
        }
        cb(node, false /* is_leaf */);
    }

    if (ret == btree_status_t::success) { cb(node, true /* is_leaf */); }
    return ret;
}

template < typename K, typename V >
void Btree< K, V >::get_all_kvs(std::vector< pair< K, V > >& kvs) const {
    post_order_traversal(locktype_t::READ, [this, &kvs](const auto& node, bool is_leaf) {
        if (!is_leaf) { node->get_all_kvs(kvs); }
    });
}

template < typename K, typename V >
btree_status_t Btree< K, V >::do_destroy(uint64_t& n_freed_nodes, void* context) {
    return post_order_traversal(locktype_t::WRITE, [this, &n_freed_nodes, context](const auto& node, bool is_leaf) {
        free_node(node, context);
        ++n_freed_nodes;
    });
}

template < typename K, typename V >
uint64_t Btree< K, V >::get_btree_node_cnt() const {
    uint64_t cnt = 1; /* increment it for root */
    m_btree_lock.lock_shared();
    cnt += get_child_node_cnt(m_root_node_id);
    m_btree_lock.unlock_shared();
    return cnt;
}

template < typename K, typename V >
uint64_t Btree< K, V >::get_child_node_cnt(bnodeid_t bnodeid) const {
    uint64_t cnt{0};
    BtreeNodePtr< K > node;
    locktype_t acq_lock = locktype_t::READ;

    if (read_and_lock_node(bnodeid, node, acq_lock, acq_lock, nullptr) != btree_status_t::success) { return cnt; }
    if (!node->is_leaf()) {
        uint32_t i = 0;
        while (i < node->get_total_entries()) {
            BtreeNodeInfo p = node->get(i, false);
            cnt += get_child_node_cnt(p.bnode_id()) + 1;
            ++i;
        }
        if (node->has_valid_edge()) { cnt += get_child_node_cnt(node->get_edge_id()) + 1; }
    }
    unlock_node(node, acq_lock);
    return cnt;
}

template < typename K, typename V >
void Btree< K, V >::to_string(bnodeid_t bnodeid, std::string& buf) const {
    BtreeNodePtr< K > node;

    locktype_t acq_lock = locktype_t::READ;

    if (read_and_lock_node(bnodeid, node, acq_lock, acq_lock, nullptr) != btree_status_t::success) { return; }
    fmt::format_to(std::back_inserter(buf), "{}\n", node->to_string(true /* print_friendly */));

    if (!node->is_leaf()) {
        uint32_t i = 0;
        while (i < node->get_total_entries()) {
            BtreeNodeInfo p;
            node->get_nth_value(i, &p, false);
            to_string(p.bnode_id(), buf);
            ++i;
        }
        if (node->has_valid_edge()) { to_string(node->get_edge_id(), buf); }
    }
    unlock_node(node, acq_lock);
}

#if 0
                btree_status_t merge_node_replay(btree_journal_entry* jentry, const btree_cp_ptr& bcp) {
                    BtreeNodePtr< K > parent_node = (jentry->is_root) ? read_node(m_root_node_id) : read_node(jentry->parent_node.node_id);

                    // Parent already went ahead of the journal entry, return done
                    if (parent_node->get_gen() >= jentry->parent_node.node_gen) { return btree_status_t::replay_not_needed; }
                }
#endif

template < typename K, typename V >
void Btree< K, V >::validate_sanity_child(const BtreeNodePtr< K >& parent_node, uint32_t ind) const {
    BtreeNodeInfo child_info;
    K child_first_key;
    K child_last_key;
    K parent_key;

    parent_node->get(ind, &child_info, false /* copy */);
    BtreeNodePtr< K > child_node = nullptr;
    auto ret = read_node(child_info.bnode_id(), child_node);
    BT_REL_ASSERT_EQ(ret, btree_status_t::success, "read failed, reason: {}", ret);
    if (child_node->get_total_entries() == 0) {
        auto parent_entries = parent_node->get_total_entries();
        if (!child_node->is_leaf()) { // leaf node or edge node can have 0 entries
            BT_REL_ASSERT_EQ(((parent_node->has_valid_edge() && ind == parent_entries)), true);
        }
        return;
    }
    child_node->get_first_key(&child_first_key);
    child_node->get_last_key(&child_last_key);
    BT_REL_ASSERT_LE(child_first_key.compare(&child_last_key), 0);
    if (ind == parent_node->get_total_entries()) {
        BT_REL_ASSERT_EQ(parent_node->has_valid_edge(), true);
        if (ind > 0) {
            parent_node->get_nth_key(ind - 1, &parent_key, false);
            BT_REL_ASSERT_GT(child_first_key.compare(&parent_key), 0);
            BT_REL_ASSERT_LT(parent_key.compare_start(&child_first_key), 0);
        }
    } else {
        parent_node->get_nth_key(ind, &parent_key, false);
        BT_REL_ASSERT_LE(child_first_key.compare(&parent_key), 0)
        BT_REL_ASSERT_LE(child_last_key.compare(&parent_key), 0)
        BT_REL_ASSERT_GE(parent_key.compare_start(&child_first_key), 0)
        BT_REL_ASSERT_GE(parent_key.compare_start(&child_first_key), 0)
        if (ind != 0) {
            parent_node->get_nth_key(ind - 1, &parent_key, false);
            BT_REL_ASSERT_GT(child_first_key.compare(&parent_key), 0)
            BT_REL_ASSERT_LT(parent_key.compare_start(&child_first_key), 0)
        }
    }
}

template < typename K, typename V >
void Btree< K, V >::validate_sanity_next_child(const BtreeNodePtr< K >& parent_node, uint32_t ind) const {
    BtreeNodeInfo child_info;
    K child_key;
    K parent_key;

    if (parent_node->has_valid_edge()) {
        if (ind == parent_node->get_total_entries()) { return; }
    } else {
        if (ind == parent_node->get_total_entries() - 1) { return; }
    }
    parent_node->get(ind + 1, &child_info, false /* copy */);

    BtreeNodePtr< K > child_node = nullptr;
    auto ret = read_node(child_info.bnode_id(), child_node);
    BT_REL_ASSERT_EQ(ret, btree_status_t::success, "read failed, reason: {}", ret);

    if (child_node->get_total_entries() == 0) {
        auto parent_entries = parent_node->get_total_entries();
        if (!child_node->is_leaf()) { // leaf node can have 0 entries
            BT_REL_ASSERT_EQ(((parent_node->has_valid_edge() && ind == parent_entries) || (ind = parent_entries - 1)),
                             true);
        }
        return;
    }
    /* in case of merge next child will never have zero entries otherwise it would have been merged */
    BT_NODE_REL_ASSERT_NE(child_node->get_total_entries(), 0, child_node);
    child_node->get_first_key(&child_key);
    parent_node->get_nth_key(ind, &parent_key, false);
    BT_REL_ASSERT_GT(child_key.compare(&parent_key), 0)
    BT_REL_ASSERT_LT(parent_key.compare_start(&child_key), 0)
}

template < typename K, typename V >
void Btree< K, V >::print_node(const bnodeid_t& bnodeid) const {
    std::string buf;
    BtreeNodePtr< K > node;

    m_btree_lock.lock_shared();
    locktype_t acq_lock = locktype_t::READ;
    if (read_and_lock_node(bnodeid, node, acq_lock, acq_lock, nullptr) != btree_status_t::success) { goto done; }
    buf = node->to_string(true /* print_friendly */);
    unlock_node(node, acq_lock);

done:
    m_btree_lock.unlock_shared();

    BT_LOG(INFO, "Node: <{}>", buf);
}

#if 0
template < typename K, typename V >
void Btree< K, V >::diff(Btree* other, uint32_t param, vector< pair< K, V > >* diff_kv) {
    std::vector< pair< K, V > > my_kvs, other_kvs;

    get_all_kvs(&my_kvs);
    other->get_all_kvs(&other_kvs);
    auto it1 = my_kvs.begin();
    auto it2 = other_kvs.begin();

    K k1, k2;
    V v1, v2;

    if (it1 != my_kvs.end()) {
        k1 = it1->first;
        v1 = it1->second;
    }
    if (it2 != other_kvs.end()) {
        k2 = it2->first;
        v2 = it2->second;
    }

    while ((it1 != my_kvs.end()) && (it2 != other_kvs.end())) {
        if (k1.preceeds(&k2)) {
            /* k1 preceeds k2 - push k1 and continue */
            diff_kv->emplace_back(make_pair(k1, v1));
            it1++;
            if (it1 == my_kvs.end()) { break; }
            k1 = it1->first;
            v1 = it1->second;
        } else if (k1.succeeds(&k2)) {
            /* k2 preceeds k1 - push k2 and continue */
            diff_kv->emplace_back(make_pair(k2, v2));
            it2++;
            if (it2 == other_kvs.end()) { break; }
            k2 = it2->first;
            v2 = it2->second;
        } else {
            /* k1 and k2 overlaps */
            std::vector< pair< K, V > > overlap_kvs;
            diff_read_next_t to_read = READ_BOTH;

            v1.get_overlap_diff_kvs(&k1, &v1, &k2, &v2, param, to_read, overlap_kvs);
            for (auto ovr_it = overlap_kvs.begin(); ovr_it != overlap_kvs.end(); ovr_it++) {
                diff_kv->emplace_back(make_pair(ovr_it->first, ovr_it->second));
            }

            switch (to_read) {
            case READ_FIRST:
                it1++;
                if (it1 == my_kvs.end()) {
                    // Add k2,v2
                    diff_kv->emplace_back(make_pair(k2, v2));
                    it2++;
                    break;
                }
                k1 = it1->first;
                v1 = it1->second;
                break;

            case READ_SECOND:
                it2++;
                if (it2 == other_kvs.end()) {
                    diff_kv->emplace_back(make_pair(k1, v1));
                    it1++;
                    break;
                }
                k2 = it2->first;
                v2 = it2->second;
                break;

            case READ_BOTH:
                /* No tail part */
                it1++;
                if (it1 == my_kvs.end()) { break; }
                k1 = it1->first;
                v1 = it1->second;
                it2++;
                if (it2 == my_kvs.end()) { break; }
                k2 = it2->first;
                v2 = it2->second;
                break;

            default:
                LOGERROR("ERROR: Getting Overlapping Diff KVS for {}:{}, {}:{}, to_read {}", k1, v1, k2, v2, to_read);
                /* skip both */
                it1++;
                if (it1 == my_kvs.end()) { break; }
                k1 = it1->first;
                v1 = it1->second;
                it2++;
                if (it2 == my_kvs.end()) { break; }
                k2 = it2->first;
                v2 = it2->second;
                break;
            }
        }
    }

    while (it1 != my_kvs.end()) {
        diff_kv->emplace_back(make_pair(it1->first, it1->second));
        it1++;
    }

    while (it2 != other_kvs.end()) {
        diff_kv->emplace_back(make_pair(it2->first, it2->second));
        it2++;
    }
}

void merge(Btree* other, match_item_cb_t< K, V > merge_cb) {
    std::vector< pair< K, V > > other_kvs;

    other->get_all_kvs(&other_kvs);
    for (auto it = other_kvs.begin(); it != other_kvs.end(); it++) {
        K k = it->first;
        V v = it->second;
        BRangeCBParam local_param(k, v);
        K start(k.start(), 1), end(k.end(), 1);

        auto search_range = BtreeSearchRange(start, true, end, true);
        BtreeUpdateRequest< K, V > ureq(search_range, merge_cb, nullptr, (BRangeCBParam*)&local_param);
        range_put(k, v, btree_put_type::APPEND_IF_EXISTS_ELSE_INSERT, nullptr, nullptr, ureq);
    }
}
#endif

#ifdef USE_STORE_TYPE
template < btree_store_type BtreeStoreType, typename K, typename V, btree_node_type InteriorNodeType,
           btree_node_type LeafNodeType >
thread_local homeds::reserve_vector< btree_locked_node_info, 5 > btree_t::wr_locked_nodes;

template < btree_store_type BtreeStoreType, typename K, typename V, btree_node_type InteriorNodeType,
           btree_node_type LeafNodeType >
thread_local homeds::reserve_vector< btree_locked_node_info, 5 > btree_t::rd_locked_nodes;
#endif

} // namespace btree
} // namespace sisl
