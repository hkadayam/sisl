#pragma once
#include "btree.ipp"

namespace sisl {
namespace btree {
#ifdef INCASE_WE_NEED_COMMON
// Common class for all membtree's
template < typename K, typename V >
class MemBtreeCommon : public BtreeCommon< K, V > {
public:
    void deref_node(BtreeNode< K, V >* node) override {
        if (node->m_refcount.decrement_testz()) {
            delete node->m_node_buf;
            delete node;
        }
    }
};

MemBtree(BtreeConfig& cfg) : Btree(update_node_area_size(cfg)) {
    Btree< K, V >::create_store_common(btree_store_type::MEM, []() { return std::make_shared< MemBtreeCommon >(); });
}
#endif

template < typename K, typename V >
class MemBtree : public Btree< K, V > {
public:
    MemBtree(BtreeConfig& cfg) : Btree(update_node_area_size(cfg)) {}

    virtual MemBtree::~MemBtree() {
        uint64_t free_node_cnt;
        auto ret = destroy(nullptr, free_node_cnt, true);

        BT_DBG_ASSERT_EQ(ret, btree_status_t::success, "btree destroy failed");
        LOGWARN("Destroy in-memory btree nodes failed.");
    }

    btree_status_t put(const BtreeKey& k, const BtreeValue& v, btree_put_type put_type, BtreeValue* existing_val) {
        return put_internal(BtreeSinglePutRequest{k, v, put_type, existing_val});
    }

    template < typename K, typename V >
    btree_status_t get(const BtreeKey& key, BtreeValue* outval) const {
        return get(key, nullptr, outval);
    }

    template < typename K, typename V >
    btree_status_t get(const BtreeKey& key, BtreeKey* outkey, BtreeValue* outval) const {
        return get_any(BtreeSearchRange(key), outkey, outval);
    }

    template < typename K, typename V >
    btree_status_t Btree< K, V >::get_any(const BtreeSearchRange& range, BtreeKey* outkey, BtreeValue* outval) const {
        return get_any(BtreeGetRequest{range, outval, outkey});
    }

    template < typename K, typename V >
    btree_status_t Btree< K, V >::remove(const BtreeKey& key, BtreeValue* outval) {
        return remove_any(BtreeSearchRange(key), nullptr, outval);
    }

    template < typename K, typename V >
    btree_status_t Btree< K, V >::remove_any(const BtreeSearchRange& range, BtreeKey* outkey,
                                             BtreeValue* outval) const {
        return remove_any(BtreeRemoveRequest{range, outval, outkey});
    }

private:
    static void update_node_area_size(BtreeConfig& cfg) {
        cfg.m_node_area_size = cfg.node_size() - sizeof(MemBtreeNode) - sizoef(LeafPhysicalNode);
    }

    BtreeNodePtr alloc_node(bool is_leaf, bool& is_new_allocation, /* is alloced same as copy_from */
                            const BtreeNodePtr& copy_from = nullptr) const override {
        if (copy_from != nullptr) {
            is_new_allocation = false;
            return copy_from;
        }

        is_new_allocation = true;
        uint8_t* node_buf = new uint8_t[m_cfg.node_size()];
        auto new_node = init_node(node_buf, bnodeid_t{0}, true, is_leaf);
        new_node->set_node_id(bnodeid_t{r_cast< std::uintptr_t >(new_node)});
        // new_node->store_type = btree_store_type::MEM;
        return BtreeNodePtr{new_node};
    }

    btree_status_t read_node(bnodeid_t id, BtreeNodePtr& bnode) const override {
        bnode = BtreeNodePtr{r_cast< BtreeNode< K, V >* >(id)};
        return btree_status_t::success;
    }

    btree_status_t write_node([[maybe_unused]] const BtreeNodePtr& bn,
                              [[maybe_unused]] const BtreeNodePtr& dependent_bn,
                              [[maybe_unused]] const BtreeRequest& req) override {
        return btree_status_t::success;
    }

    void swap_node(const BtreeNodePtr& node1, const BtreeNodePtr& node2) const override {
        std::swap(node1->m_node_buf, node2->m_node_buf);
    }

    btree_status_t refresh_node(const BtreeNodePtr& bn, bool is_write_modifiable) override {
        return btree_status_t::success;
    }

    void split_node_precommit(const BtreeNodePtr& parent_node, const BtreeNodePtr& child_node1,
                              const BtreeNodePtr& child_node2, bool root_split, void* context) override {}
#if 0
    static void ref_node(MemBtreeNode* bn) {
        auto mbh = (mem_btree_node_header*)bn;
        LOGMSG_ASSERT_EQ(mbh->magic, 0xDEADBEEF, "Invalid Magic for Membtree node {}, Metrics {}", bn->to_string(),
                         sisl::MetricsFarm::getInstance().get_result_in_json_string());
        mbh->refcount.increment();
    }

    static void deref_node(MemBtreeNode* bn) {
        auto mbh = (mem_btree_node_header*)bn;
        LOGMSG_ASSERT_EQ(mbh->magic, 0xDEADBEEF, "Invalid Magic for Membtree node {}, Metrics {}", bn->to_string(),
                         sisl::MetricsFarm::getInstance().get_result_in_json_string());
        if (mbh->refcount.decrement_testz()) {
            mbh->magic = 0;
            bn->~MemBtreeNode();
            deallocate_mem((uint8_t*)bn);
        }
    }
#endif
};

} // namespace btree
} // namespace sisl
