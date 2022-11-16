/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Author/Developer(s): Harihara Kadayam, Rishabh Mittal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once

#include <string>
#include <vector>
#include <fmt/format.h>
#include "../fds/buffer.hpp"

namespace sisl {
namespace btree {

ENUM(MultiMatchOption, uint16_t,
     DO_NOT_CARE, // Select anything that matches
     LEFT_MOST,   // Select the left most one
     RIGHT_MOST,  // Select the right most one
     MID          // Select the middle one
)

ENUM(btree_put_type, uint16_t,
     INSERT_ONLY_IF_NOT_EXISTS,     // Insert
     REPLACE_ONLY_IF_EXISTS,        // Update
     REPLACE_IF_EXISTS_ELSE_INSERT, // Upsert
     APPEND_ONLY_IF_EXISTS,         // Update
     APPEND_IF_EXISTS_ELSE_INSERT)

// The base class, btree library expects its key to be derived from
class BtreeKey {
public:
    BtreeKey() = default;

    // Deleting copy constructor forces the derived class to define its own copy constructor
    // BtreeKey(const BtreeKey& other) = delete;
    // BtreeKey(const sisl::blob& b) = delete;
    BtreeKey(const BtreeKey& other) = default;
    virtual ~BtreeKey() = default;

    virtual BtreeKey& operator=(const BtreeKey& other) {
        clone(other);
        return *this;
    };

    virtual void clone(const BtreeKey& other) = 0;
    virtual int compare(const BtreeKey& other) const = 0;

    virtual sisl::blob serialize() const = 0;
    virtual uint32_t serialized_size() const = 0;
    // virtual void deserialize(const sisl::blob& b) = 0;

    virtual std::string to_string() const = 0;
    virtual bool is_extent_key() const { return false; }
};

template < typename K >
class BtreeTraversalState;

template < typename K >
class BtreeKeyRange {
private:
    K m_actual_start_key;
    K m_actual_end_key;

public:
    K* m_input_start_key{&m_actual_start_key};
    K* m_input_end_key{&m_actual_end_key};
    bool m_start_incl{true};
    bool m_end_incl{true};
    MultiMatchOption m_multi_selector{MultiMatchOption::DO_NOT_CARE};

    friend class BtreeTraversalState< K >;

public:
    BtreeKeyRange() = default;

    BtreeKeyRange(const K& start_key, bool start_incl = true) :
            m_actual_start_key{start_key},
            m_input_start_key{&m_actual_start_key},
            m_input_end_key{&m_actual_start_key},
            m_start_incl{start_incl},
            m_end_incl{true},
            m_multi_selector{MultiMatchOption::DO_NOT_CARE} {}

    BtreeKeyRange(const K& start_key, bool start_incl, const K& end_key, bool end_incl = true,
                  MultiMatchOption option = MultiMatchOption::DO_NOT_CARE) :
            m_actual_start_key{start_key},
            m_actual_end_key{end_key},
            m_input_start_key{&m_actual_start_key},
            m_input_end_key{&m_actual_end_key},
            m_start_incl{start_incl},
            m_end_incl{end_incl},
            m_multi_selector{option} {}

    BtreeKeyRange(const K& start_key, const K& end_key) : BtreeKeyRange(start_key, true, end_key, true) {}

    BtreeKeyRange(const BtreeKeyRange& other) { copy(other); }
    BtreeKeyRange(BtreeKeyRange&& other) { do_move(std::move(other)); }
    BtreeKeyRange& operator=(const BtreeKeyRange< K >& other) {
        this->copy(other);
        return *this;
    }
    BtreeKeyRange& operator=(BtreeKeyRange< K >&& other) {
        this->do_move(std::move(other));
        return *this;
    }

    void copy(const BtreeKeyRange< K >& other) {
        m_actual_start_key = other.m_actual_start_key;
        m_actual_end_key = other.m_actual_end_key;
        m_input_start_key = &m_actual_start_key;
        m_input_end_key =
            (other.m_input_end_key == &other.m_actual_start_key) ? &m_actual_start_key : &m_actual_end_key;
        m_start_incl = other.m_start_incl;
        m_end_incl = other.m_end_incl;
        m_multi_selector = other.m_multi_selector;
    }

    void do_move(BtreeKeyRange< K >&& other) {
        m_input_start_key = &m_actual_start_key;
        m_input_end_key =
            (other.m_input_end_key == &other.m_actual_start_key) ? &m_actual_start_key : &m_actual_end_key;
        m_actual_start_key = std::move(other.m_actual_start_key);
        m_actual_end_key = std::move(other.m_actual_end_key);
        m_start_incl = std::move(other.m_start_incl);
        m_end_incl = std::move(other.m_end_incl);
        m_multi_selector = std::move(other.m_multi_selector);
    }

    void set_multi_option(MultiMatchOption o) { m_multi_selector = o; }
    const K& start_key() const { return *m_input_start_key; }
    const K& end_key() const { return *m_input_end_key; }
    bool is_start_inclusive() const { return m_start_incl; }
    bool is_end_inclusive() const { return m_end_incl; }
    MultiMatchOption multi_option() const { return m_multi_selector; }

    void set_end_key(K&& key, bool incl) {
        m_actual_end_key = std::move(key);
        m_end_incl = incl;
    }

    std::string to_string() const {
        return fmt::format("{}{}-{}{}", is_start_inclusive() ? '[' : '(', start_key().to_string(),
                           end_key().to_string(), is_end_inclusive() ? ']' : ')');
    }

private:
    const K& actual_start_key() const { return m_actual_start_key; }
    const K& actual_end_key() const { return m_actual_end_key; }
};

/*
class BtreeKeyRange {
public:
    const BtreeKey* m_input_start_key{nullptr};
    const BtreeKey* m_input_end_key{nullptr};
    bool m_start_incl;
    bool m_end_incl;
    MultiMatchOption m_multi_selector;

    friend class BtreeTraversalState;

    template < typename K >
    friend class BtreeKeyRangeSafe;

    BtreeKeyRange(const BtreeKeyRange& other) = default;
    BtreeKeyRange& operator=(const BtreeKeyRange& other) = default;

    void set_multi_option(MultiMatchOption o) { m_multi_selector = o; }
    virtual const BtreeKey& start_key() const { return *m_input_start_key; }
    virtual const BtreeKey& end_key() const { return *m_input_end_key; }

    virtual bool is_start_inclusive() const { return m_start_incl; }
    virtual bool is_end_inclusive() const { return m_end_incl; }
    virtual bool is_simple_search() const {
        return ((m_input_start_key == m_input_end_key) && (m_start_incl == m_end_incl));
    }
    MultiMatchOption multi_option() const { return m_multi_selector; }

private:
    BtreeKeyRange(const BtreeKey* start_key, bool start_incl, const BtreeKey* end_key, bool end_incl,
                  MultiMatchOption option) :
            m_input_start_key{start_key},
            m_input_end_key{end_key},
            m_start_incl{start_incl},
            m_end_incl{end_incl},
            m_multi_selector{option} {}
    BtreeKeyRange(const BtreeKey* start_key, bool start_incl, MultiMatchOption option) :
            m_input_start_key{start_key},
            m_input_end_key{start_key},
            m_start_incl{start_incl},
            m_end_incl{start_incl},
            m_multi_selector{option} {}
};
*/
/*
 * This type is for keys which is range in itself i.e each key is having its own
 * start() and end().
 */
template < typename K >
class ExtentBtreeKey : public BtreeKey {
public:
    ExtentBtreeKey() = default;
    virtual ~ExtentBtreeKey() = default;
    virtual bool is_extent_key() const { return true; }

    // Provide the length of the extent key, which is end - start + 1
    virtual uint32_t extent_length() const = 0;

    // Get the distance between the start of this key and start of other key. It returns equivalent of
    // (other.start - this->start + 1)
    virtual int64_t distance_start(const ExtentBtreeKey< K >& other) const = 0;

    // Get the distance between the end of this key and end of other key. It returns equivalent of
    // (other.end - this->end + 1)
    virtual int64_t distance_end(const ExtentBtreeKey< K >& other) const = 0;

    // Get the distance between the start of this key and end of other key. It returns equivalent of
    // (other.end - this->start + 1)
    virtual int64_t distance(const ExtentBtreeKey< K >& other) const = 0;

    // Extract a new extent key from the given offset upto this length from this key and optionally do a deep copy
    virtual K extract(uint32_t offset, uint32_t length, bool copy) const = 0;

    // Merge this extent btree key with other extent btree key and return a new key
    virtual K combine(const ExtentBtreeKey< K >& other) const = 0;

    // TODO: Evaluate if we need these 3 methods or we can manage with other methods
    virtual int compare_start(const BtreeKey& other) const = 0;
    virtual int compare_end(const BtreeKey& other) const = 0;

    /* we always compare the end key in case of extent */
    virtual int compare(const BtreeKey& other) const override { return (compare_end(other)); }

    K extract_end(bool copy) const { return extract(extent_length() - 1, 1, copy); }
};

class BtreeValue {
public:
    BtreeValue() = default;
    virtual ~BtreeValue() = default;

    // Deleting copy constructor forces the derived class to define its own copy constructor
    BtreeValue(const BtreeValue& other) = delete;

    virtual blob serialize() const = 0;
    virtual uint32_t serialized_size() const = 0;
    virtual void deserialize(const blob& b, bool copy) = 0;

    virtual std::string to_string() const { return ""; }
};

template < typename V >
class ExtentBtreeValue : public BtreeValue {
public:
    virtual ~ExtentBtreeValue() = default;

    // Extract a new extent value from the given offset upto this length from this value and optionally do a deep copy
    virtual V extract(uint32_t offset, uint32_t length, bool copy) const = 0;

    // Returns the returns the serialized size if we were to extract other value from offset upto length
    // This method is equivalent to: extract(offset, length, false).serialized_size()
    // However, this method provides values to directly compute the extracted size without extracting - which is more
    // efficient.
    virtual uint32_t extracted_size(uint32_t offset, uint32_t length) const = 0;

    // This method is similar to extract(0, length) along with moving the current values start to length. So for example
    // if value has 0-100 and if shift(80) is called, this method returns a value from 0-79 and moves the start offset
    // of current value to 80.
    virtual V shift(uint32_t length, bool copy) = 0;

    // Given the length, report back how many extents from the current value can fit.
    virtual uint32_t num_extents_fit(uint32_t length) const = 0;

    // Returns if every piece of extents are equally sized.
    virtual bool is_equal_sized() const = 0;
};

#if 0
template < typename K >
class BtreeKeyRangeSafe : public BtreeKeyRange {
public:
    K m_actual_start_key;
    K m_actual_end_key;

public:
    BtreeKeyRangeSafe(const BtreeKey& start_key) :
            BtreeKeyRange(nullptr, true, nullptr, true, MultiMatchOption::DO_NOT_CARE), m_actual_start_key{start_key} {
        this->m_input_start_key = &m_actual_start_key;
        this->m_input_end_key = &m_actual_start_key;
    }

    virtual ~BtreeKeyRangeSafe() = default;

    BtreeKeyRangeSafe(const BtreeKey& start_key, const BtreeKey& end_key) :
            BtreeKeyRangeSafe(start_key, true, end_key, true) {}

    BtreeKeyRangeSafe(const BtreeKey& start_key, bool start_incl, const BtreeKey& end_key, bool end_incl,
                      MultiMatchOption option = MultiMatchOption::DO_NOT_CARE) :
            BtreeKeyRange(nullptr, start_incl, nullptr, end_incl, option),
            m_actual_start_key{start_key},
            m_actual_end_key{end_key} {
        this->m_input_start_key = &m_actual_start_key;
        this->m_input_end_key = &m_actual_end_key;
    }

    BtreeKeyRangeSafe(const BtreeKeyRange& other) :
            BtreeKeyRange(nullptr, other.is_start_inclusive(), nullptr, other.is_end_inclusive(),
                          other.multi_option()) {
        m_actual_start_key = *other.m_input_start_key;
        this->m_input_start_key = &m_actual_start_key;

        if (other.m_input_start_key != other.m_input_end_key) {
            m_actual_end_key = *other.m_input_end_key;
            this->m_input_end_key = &m_actual_end_key;
        } else {
            this->m_input_end_key = &m_actual_start_key;
        }
    }

    BtreeKeyRangeSafe& operator=(const BtreeKeyRange& other) {
        this->m_start_incl = other.m_start_incl;
        this->m_end_incl = other.m_end_incl;
        this->m_multi_selector = other.m_multi_selector;
        m_actual_start_key = *other.m_input_start_key;
        this->m_input_start_key = &m_actual_start_key;

        if (other.m_input_start_key != other.m_input_end_key) {
            m_actual_end_key = *other.m_input_end_key;
            this->m_input_end_key = &m_actual_end_key;
        } else {
            this->m_input_end_key = &m_actual_start_key;
        }
        return *this;
    }

    /******************* all functions are constant *************/
    BtreeKeyRangeSafe< K > start_of_range() const {
        return BtreeKeyRangeSafe< K >(start_key(), is_start_inclusive(), multi_option());
    }
    BtreeKeyRangeSafe< K > end_of_range() const {
        return BtreeKeyRangeSafe< K >(end_key(), is_end_inclusive(), multi_option());
    }
};
#endif

struct BtreeLockTracker;
template < typename K >
struct BtreeQueryCursor {
    std::unique_ptr< K > m_last_key;
    std::unique_ptr< BtreeLockTracker > m_locked_nodes;
    BtreeQueryCursor() = default;

    const sisl::blob serialize() const { return m_last_key ? m_last_key->serialize() : sisl::blob{}; };
    virtual std::string to_string() const { return (m_last_key) ? m_last_key->to_string() : "null"; }
};

// This class holds the current state of the search. This is where intermediate search state are stored
// and it is mutated by the do_put and do_query methods. Expect the current_sub_range and cursor to keep
// getting updated on calls.
template < typename K >
class BtreeTraversalState {
protected:
    const BtreeKeyRange< K > m_input_range;
    BtreeKeyRange< K > m_working_range;
    BtreeKeyRange< K > m_next_range;
    std::unique_ptr< BtreeQueryCursor< K > > m_cursor;

public:
    BtreeTraversalState(BtreeKeyRange< K >&& inp_range, bool paginated_query = false) :
            m_input_range{std::move(inp_range)}, m_working_range{m_input_range} {
        if (paginated_query) { m_cursor = std::make_unique< BtreeQueryCursor< K > >(); }
    }
    BtreeTraversalState(const BtreeTraversalState& other) = default;
    BtreeTraversalState(BtreeTraversalState&& other) = default;

    const BtreeQueryCursor< K >* const_cursor() const { return m_cursor.get(); }
    BtreeQueryCursor< K >* cursor() { return m_cursor.get(); }
    bool is_cursor_valid() const { return (m_cursor != nullptr); }

    void set_cursor_key(const K& end_key) {
        // no need to set cursor as user doesn't want to keep track of it
        if (!m_cursor) { return; }
        m_cursor->m_last_key = std::make_unique< K >(end_key);
    }

    const BtreeKeyRange< K >& input_range() const { return m_input_range; }
    const BtreeKeyRange< K >& working_range() const { return m_working_range; }

    // Returns the mutable reference to the end key, which caller can update it to trim down the end key
    void trim_working_range(K&& end_key, bool end_incl) { m_working_range.set_end_key(std::move(end_key), end_incl); }

    const K& next_key() const {
        return (m_cursor && m_cursor->m_last_key) ? *m_cursor->m_last_key : m_input_range.start_key();
    }

    const BtreeKeyRange< K >& next_range() {
        if (m_cursor && m_cursor->m_last_key) {
            m_next_range = BtreeKeyRange< K >(*m_cursor->m_last_key, false, m_input_range.end_key(), is_end_inclusive(),
                                              m_input_range.multi_option());
            return m_next_range;
        } else {
            return m_input_range;
        }
    }

private:
    bool is_start_inclusive() const {
        // cursor always have the last key not included
        return (m_cursor && m_cursor->m_last_key) ? false : m_input_range.is_start_inclusive();
    }

    bool is_end_inclusive() const { return m_input_range.is_end_inclusive(); }
};

#pragma pack(1)
class BtreeLinkInfo : public BtreeValue {
private:
    bnodeid_t m_bnodeid{empty_bnodeid};
    uint64_t m_link_version{0}; // Link version between parent and a child

public:
    BtreeLinkInfo() = default;
    explicit BtreeLinkInfo(bnodeid_t id, uint64_t v) : m_bnodeid(id), m_link_version{v} {}
    BtreeLinkInfo& operator=(const BtreeLinkInfo& other) = default;

    bnodeid_t bnode_id() const { return m_bnodeid; }
    uint64_t link_version() const { return m_link_version; }
    void set_bnode_id(bnodeid_t bid) { m_bnodeid = bid; }
    void set_link_version(uint64_t v) { m_link_version = v; }
    bool has_valid_bnode_id() const { return (m_bnodeid != empty_bnodeid); }

    sisl::blob serialize() const override {
        sisl::blob b;
        b.size = sizeof(BtreeLinkInfo);
        b.bytes = uintptr_cast(const_cast< BtreeLinkInfo* >(this));
        return b;
    }
    uint32_t serialized_size() const override { return sizeof(BtreeLinkInfo); }
    static uint32_t get_fixed_size() { return sizeof(BtreeLinkInfo); }
    std::string to_string() const override { return fmt::format("{}.{}", m_bnodeid, m_link_version); }

    void deserialize(const blob& b, bool copy) override {
        DEBUG_ASSERT_EQ(b.size, sizeof(BtreeLinkInfo), "BtreeLinkInfo deserialize received invalid blob");
        BtreeLinkInfo* other = r_cast< BtreeLinkInfo* >(b.bytes);
        m_bnodeid = other->m_bnodeid;
        m_link_version = other->m_link_version;
    }

    friend std::ostream& operator<<(std::ostream& os, const BtreeLinkInfo& b) {
        os << b.to_string();
        return os;
    }
};
#pragma pack()

} // namespace btree
} // namespace sisl
