/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 * Author/Developer(s): Harihara Kadayam
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
#include <iostream>
#include <gtest/gtest.h>
#include <string>
#include <random>
#include <filesystem>
#include <cstdint>

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#endif

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <sisl/utility/enum.hpp>
#include <sisl/cache/simple_cache.hpp>
#include <sisl/cache/lru_evictor.hpp>

using namespace sisl;
static constexpr uint32_t g_val_size{512};
static thread_local std::random_device g_rd{};
static thread_local std::default_random_engine g_re{g_rd()};

struct Entry {
    Entry(uint32_t id, const std::string& contents = "") : m_id{id}, m_contents{contents} {}

    uint32_t m_id;
    std::string m_contents;
};

static std::string gen_value(uint32_t key, size_t len) {
    std::stringstream ss;
    ss << std::hex << std::setw(16) << std::setfill('0') << len;

    auto const num_words = (len - sizeof(size_t)) / sizeof(uint32_t);
    for (size_t i{0}; i < num_words; ++i) {
        ss << std::hex << std::setw(8) << std::setfill('0') << key;
    }
    return ss.str();
}

static bool validate_value(uint32_t key, std::string const& value) {
    // uint64_t len_read = std::stoul(value.substr(0, 16), nullptr, 16);
    uint32_t key_read1 = std::stoul(value.substr(16, 8), nullptr, 16);
    uint32_t key_read2 = std::stoul(value.substr(16, 8), nullptr, 16);

    return ((key_read1 == key) && (key_read2 == key));
}

struct SimpleCacheTest : public testing::Test {
protected:
    std::shared_ptr< Evictor > m_evictor;
    std::unique_ptr< SimpleCache< uint32_t, std::shared_ptr< Entry > > > m_cache;

    // We can't always maintain shadow, especially for multi-threaded case
    std::set< uint32_t > m_existing_keys;
    bool m_maintain_shadow{true};

    std::atomic< uint64_t > m_num_cache_entries{0};
    std::atomic< uint64_t > m_cache_misses{0};
    std::atomic< uint64_t > m_cache_hits{0};
    std::atomic< uint64_t > m_eviction_count{0};
    std::atomic< uint64_t > m_nread_ops{0};
    std::atomic< uint64_t > m_nwrite_ops{0};
    std::atomic< uint64_t > m_nremove_ops{0};

    uint32_t m_max_cached_keys;
    uint32_t m_max_keys;

protected:
    void SetUp() override {
        const auto store_size = SISL_OPTIONS["store_size_mb"].as< uint32_t >() * 1024 * 1024;
        const auto cache_pct = SISL_OPTIONS["cache_pct"].as< uint32_t >();
        const auto cache_size = (store_size * cache_pct) / 10;
        m_max_keys = store_size / g_val_size;
        m_max_cached_keys = cache_size / g_val_size;
        LOGINFO("Initializing store_size={} MB, cache_pct={}, cache_size={} max_keys={} max_cached_keys={}", store_size,
                cache_pct, cache_size, m_max_keys, m_max_cached_keys);

        m_evictor = std::make_unique< LRUEvictor >(cache_size, 8);
        m_cache = std::make_unique< SimpleCache< uint32_t, std::shared_ptr< Entry > > >(
            m_evictor,                                                              // Evictor to evict used entries
            cache_size / 4096,                                                      // Total number of buckets
            [](const std::shared_ptr< Entry >& e) -> uint32_t { return e->m_id; },  // Method to extract key
            [](const std::shared_ptr< Entry >&) -> uint32_t { return g_val_size; }, // Method to extract size
            [this](const CacheRecord& rec) -> bool {                                // Method to prevent eviction
                if (m_maintain_shadow) {
                    const auto& hnode = (sisl::SingleEntryHashNode< std::shared_ptr< Entry > >&)rec;
                    m_existing_keys.erase(hnode.m_value->m_id);
                }
                ++m_eviction_count;
                --m_num_cache_entries;
                return true;
            });
    }

    void TearDown() override {
        m_evictor.reset();
        m_cache.reset();
    }

    void write(uint32_t id) {
        const std::string data = gen_value(id, g_val_size);
        LOGTRACE("Inserting {}", id);

        std::set< uint32_t >::iterator it;
        bool expected_insert{true};
        if (m_maintain_shadow) { std::tie(it, expected_insert) = m_existing_keys.insert(id); }

        auto status = m_cache->update(std::make_shared< Entry >(id, data));
        if (m_maintain_shadow) {
            ASSERT_EQ(status, expected_insert ? SimpleCacheStatus::not_found : SimpleCacheStatus::success)
                << "Mismatch about existence of key=" << id << " between shadow_map and cache";
        }

        if (status == SimpleCacheStatus::not_found) {
            status = m_cache->insert(std::make_shared< Entry >(id, data));
            if (m_maintain_shadow) {
                ASSERT_EQ(status, SimpleCacheStatus::success)
                    << "Mismatch about existence of key=" << id << " between shadow_map and cache";
            }
            ++m_num_cache_entries;
        }
        ++m_nwrite_ops;
    }

    void read(uint32_t id, bool insert_if_missing = false) {
        bool expected_found{true};
        if (m_maintain_shadow) { expected_found = (m_existing_keys.find(id) != m_existing_keys.end()); }

        LOGTRACE("Getting {}", id);
        std::shared_ptr< Entry > e = std::make_shared< Entry >(0);
        auto status = m_cache->get(id, e);
        if (status == SimpleCacheStatus::success) {
            if (m_maintain_shadow) {
                ASSERT_EQ(expected_found, true) << "Object key=" << id << " is deleted, but still found in cache";
            }
            ASSERT_TRUE(validate_value(id, e->m_contents)) << "Contents for key=" << id << " mismatch";
            ++m_cache_hits;
        } else {
            ++m_cache_misses;
            if (insert_if_missing) { write(id); }
        }
        ++m_nread_ops;
    }

    void remove(uint32_t id) {
        std::set< uint32_t >::iterator it;
        bool expected_found{true};

        if (m_maintain_shadow) {
            it = m_existing_keys.find(id);
            expected_found = (it != m_existing_keys.end());
        }

        std::shared_ptr< Entry > removed_e = std::make_shared< Entry >(0);
        LOGTRACE("Removing {}", id);
        bool removed = (m_cache->remove(id, removed_e) == SimpleCacheStatus::success);
        if (removed) {
            if (m_maintain_shadow) {
                ASSERT_EQ(expected_found, true)
                    << "Object for key=" << id << " is deleted already, but still found in cache";
            }
            ASSERT_TRUE(validate_value(id, removed_e->m_contents))
                << "Contents for key=" << id << " mismatch prior to removal";
            --m_num_cache_entries;
        } else {
            if (m_maintain_shadow) {
                ASSERT_EQ(expected_found, false)
                    << "Object for key=" << id << " is present in shadow, but not in cache";
            }
        }

        if (m_maintain_shadow) { m_existing_keys.erase(id); }
        ++m_nremove_ops;
    }
};

VENUM(op_t, uint8_t, READ = 0, WRITE = 1, REMOVE = 2)

TEST_F(SimpleCacheTest, SingleThreadedCacheOps) {
    this->m_maintain_shadow = true;

    static std::uniform_int_distribution< uint8_t > op_generator{0, 2};
    static std::uniform_int_distribution< uint32_t > key_generator{0, this->m_max_cached_keys};

    auto const num_ops = SISL_OPTIONS["num_ops"].as< uint32_t >();
    LOGINFO("INFO: Do random read/write operations on all chunks for {} iters", num_ops);
    for (uint32_t i{0}; i < num_ops; ++i) {
        const op_t op = s_cast< op_t >(op_generator(g_re));
        const uint32_t id = key_generator(g_re);

        switch (op) {
        case op_t::READ:
            read(id);
            break;
        case op_t::WRITE:
            write(id);
            break;
        case op_t::REMOVE:
            remove(id);
            break;
        }
    }
    LOGINFO("Executed read_ops={}, write_ops={} remove_ops={}", m_nread_ops.load(), m_nwrite_ops.load(),
            m_nremove_ops.load());
    LOGINFO("ReadCacheHits={} ({}%) ReadCacheMisses={} ({}%) Evicted={} CacheEntryCount={}", m_cache_hits.load(),
            (100 * (double)m_cache_hits.load()) / m_nread_ops.load(), m_cache_misses.load(),
            (100 * (double)m_cache_misses.load()) / m_nread_ops.load(), m_eviction_count.load(),
            m_num_cache_entries.load());
}

TEST_F(SimpleCacheTest, MultithreadedEviction) {
    this->m_maintain_shadow = false;

    static std::uniform_int_distribution< uint8_t > op_generator{0, 99};
    static std::uniform_int_distribution< uint32_t > key_generator{0, this->m_max_keys};

    // First preload entries to fill the cache
    auto const num_threads = SISL_OPTIONS["num_threads"].as< uint32_t >();
    std::vector< std::thread > threads;
    uint32_t start_key{0};
    for (uint32_t t{0}; t < num_threads; ++t) {
        auto count = m_max_cached_keys / num_threads;
        if (t == 0) { count += m_max_cached_keys % num_threads; }
        threads.emplace_back([this, start_key, count]() {
            for (uint32_t i{start_key}; i < start_key + count; ++i) {
                write(i);
                ASSERT_LE(r_cast< LRUEvictor* >(m_evictor.get())->filled_size(), m_evictor->max_size())
                    << "Cache size exceeded its limits";
            }
        });
        start_key += count;
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
    LOGINFO("Preloaded {} entries, Evicted {} entries in {} threads", m_num_cache_entries.load(),
            m_eviction_count.load(), num_threads);

    m_nwrite_ops = 0;
    auto const num_ops = SISL_OPTIONS["num_ops"].as< uint32_t >();
    LOGINFO("INFO: Do random read/write operations on all chunks for {} iters", num_ops);
    for (uint32_t t{0}; t < num_threads; ++t) {
        auto count = num_ops / num_threads;
        if (t == 0) { count += num_ops % num_threads; }

        threads.emplace_back([this, count]() {
            for (uint32_t i{0}; i < count; ++i) {
                uint8_t op_val = op_generator(g_re);
                const uint32_t id = key_generator(g_re);
                if (op_val < 25) { // 25% write, 60% reads, 15% removes
                    write(id);
                } else if (op_val < 85) {
                    read(id, /*insert_if_missing=*/true);
                } else {
                    remove(id);
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    LOGINFO("Executed read_ops={}, write_ops={} remove_ops={} in {} threads", m_nread_ops.load(), m_nwrite_ops.load(),
            m_nremove_ops.load(), num_threads);
    LOGINFO("ReadCacheHits={} ({}%) ReadCacheMisses={} ({}%) Evicted={} CacheEntryCount={}", m_cache_hits.load(),
            (100 * (double)m_cache_hits.load()) / m_nread_ops.load(), m_cache_misses.load(),
            (100 * (double)m_cache_misses.load()) / m_nread_ops.load(), m_eviction_count.load(),
            m_num_cache_entries.load());
}

#if 0
TEST_F(SimpleCacheTest, MultiThreadedWithEviction) {
    const auto num_threads = 20;
    LOGINFO("INFO: Do random read/write operations on all chunks for {} threads", num_threads);
    std::vector< std::thread > threads;
    for (uint32_t i{0}; i < num_threads; ++i) {
        threads.emplace_back([this]() {
            for (uint32_t j{0}; j < 20000; ++j) {
                const uint32_t id = g_re() % m_max_cached_keys;
                const op_t op = s_cast< op_t >(g_re() % 3);
                std::shared_ptr< Entry > e = std::make_shared< Entry >(0);
                switch (op) {
                case op_t::READ:
                    m_cache->get(id, e);
                    break;
                case op_t::WRITE:
                    m_cache->insert(std::make_shared< Entry >(id, fmt::format("test{}", j)));
                    break;
                case op_t::REMOVE:
                    m_cache->remove(id, e);
                    break;
                }
            }
        });
    }

    for (uint32_t i{0}; i < num_threads; ++i) {
        threads.emplace_back([this]() {
            for (uint32_t j{0}; j < 20000; ++j) {
                const uint32_t id = g_re() % m_max_cached_keys;
                const op_t op = s_cast< op_t >(g_re() % 3);
                std::shared_ptr< Entry > e = std::make_shared< Entry >(0);
                switch (op) {
                case op_t::READ:
                    m_cache->get(id, e);
                    break;
                case op_t::WRITE:
                    m_cache->insert(std::make_shared< Entry >(id, fmt::format("test{}", j)));
                    break;
                case op_t::REMOVE:
                    m_cache->remove(id, e);
                    break;
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
}

TEST(SimpleCacheSize, TriggerEvict) {
    uint32_t num_partitions = 10;
    uint32_t max_nodes_per_partition = 3;
    uint32_t cache_size = g_val_size * num_partitions * max_nodes_per_partition;
    std::shared_ptr< Evictor > evictor = std::make_unique< LRUEvictor >(cache_size, num_partitions);
    auto simple_cache = std::make_unique< SimpleCache< uint32_t, std::shared_ptr< Entry > > >(
        evictor,                                                               // Evictor to evict used entries
        10000,                                                                 // Total number of buckets
        g_val_size,                                                            // Value size
        [](const std::shared_ptr< Entry >& e) -> uint32_t { return e->m_id; }, // Method to extract key
        nullptr                                                                // Method to prevent eviction
    );
    auto* evictor_ptr = dynamic_cast< LRUEvictor* >(evictor.get());
    uint32_t num_iters = num_partitions * max_nodes_per_partition * 1000;
    for (uint32_t i = 0; i < num_iters; i++) {
        ASSERT_TRUE(simple_cache->insert(std::make_shared< Entry >(i, fmt::format("test{}", i))));
        ASSERT_LE(evictor_ptr->filled_size(), cache_size);
    }
    uint32_t cache_hits{0};
    for (uint32_t i = 0; i < num_iters; i++) {
        std::shared_ptr< Entry > e = std::make_shared< Entry >(0);
        if (simple_cache->get(i, e)) { ++cache_hits; }
    }
}

#endif

SISL_OPTIONS_ENABLE(logging, test_simplecache)
SISL_OPTION_GROUP(test_simplecache,
                  (store_size_mb, "", "store_size_mb", "Store size in mb to simulate",
                   ::cxxopts::value< uint32_t >()->default_value("100"), "number"),
                  (cache_pct, "", "cache_pct", "percentage of data is cached",
                   ::cxxopts::value< uint32_t >()->default_value("5"), "number"),
                  (num_ops, "", "num_ops", "number of iterations for rand ops",
                   ::cxxopts::value< uint32_t >()->default_value("65536"), "number"),
                  (num_threads, "", "num_threads", "number of threads for multi-threaded tests",
                   ::cxxopts::value< uint32_t >()->default_value("8"), "number"))

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging, test_simplecache)
    sisl::logging::SetLogger("test_simplecache");
    spdlog::set_pattern("[%D %T%z] [%^%L%$] [%t] %v");

    auto ret = RUN_ALL_TESTS();
    return ret;
}
