#ifndef DIASPORA_PFS_DRIVER_INDEX_CACHE_HPP
#define DIASPORA_PFS_DRIVER_INDEX_CACHE_HPP

#include <pfs/PartitionFiles.hpp>
#include <unordered_map>
#include <list>
#include <mutex>
#include <optional>
#include <cstdint>

namespace pfs {

/**
 * LRU cache for partition index entries.
 * Stores recently accessed index entries to avoid repeated disk reads.
 * Thread-safe for concurrent access.
 */
class IndexCache {
public:
    using IndexEntry = PartitionFiles::IndexEntry;

    /**
     * Constructor
     * @param max_entries Maximum number of entries to cache (default: 1024 = 32KB)
     */
    explicit IndexCache(size_t max_entries = 1024);

    /**
     * Get an index entry from cache
     * @param event_id Event ID to lookup
     * @return IndexEntry if found, std::nullopt if not in cache
     */
    std::optional<IndexEntry> get(uint64_t event_id);

    /**
     * Put an index entry into cache
     * @param event_id Event ID
     * @param entry Index entry to cache
     */
    void put(uint64_t event_id, const IndexEntry& entry);

    /**
     * Clear all cached entries
     */
    void clear();

    /**
     * Get cache statistics
     */
    struct Stats {
        size_t total_gets = 0;
        size_t cache_hits = 0;
        size_t cache_misses = 0;
        size_t total_puts = 0;
        size_t evictions = 0;
        size_t current_size = 0;

        double hit_rate() const {
            return total_gets > 0 ? static_cast<double>(cache_hits) / total_gets : 0.0;
        }
    };

    Stats getStats() const;

private:
    size_t m_max_entries;

    // LRU list: most recently used at front, least recently used at back
    using LRUList = std::list<uint64_t>;
    LRUList m_lru_list;

    // Map: event_id -> (IndexEntry, iterator into LRU list)
    struct CacheEntry {
        IndexEntry entry;
        LRUList::iterator lru_iter;
    };
    std::unordered_map<uint64_t, CacheEntry> m_cache;

    // Thread safety
    mutable std::mutex m_mutex;

    // Statistics
    mutable std::mutex m_stats_mutex;
    Stats m_stats;

    /**
     * Move an entry to the front of the LRU list (mark as most recently used)
     */
    void touchEntry(uint64_t event_id, LRUList::iterator lru_iter);

    /**
     * Evict the least recently used entry if cache is full
     */
    void evictIfNeeded();
};

}

#endif
