#include "pfs/IndexCache.hpp"

namespace pfs {

IndexCache::IndexCache(size_t max_entries)
    : m_max_entries(max_entries)
{
}

std::optional<IndexCache::IndexEntry> IndexCache::get(uint64_t event_id) {
    std::lock_guard<std::mutex> lock(m_mutex);

    // Update stats
    {
        std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
        m_stats.total_gets++;
    }

    auto it = m_cache.find(event_id);
    if (it == m_cache.end()) {
        // Cache miss
        std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
        m_stats.cache_misses++;
        return std::nullopt;
    }

    // Cache hit - move to front of LRU list (most recently used)
    touchEntry(event_id, it->second.lru_iter);

    {
        std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
        m_stats.cache_hits++;
    }

    return it->second.entry;
}

void IndexCache::put(uint64_t event_id, const IndexEntry& entry) {
    std::lock_guard<std::mutex> lock(m_mutex);

    // Update stats
    {
        std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
        m_stats.total_puts++;
    }

    // Check if already in cache
    auto it = m_cache.find(event_id);
    if (it != m_cache.end()) {
        // Update existing entry and move to front
        it->second.entry = entry;
        touchEntry(event_id, it->second.lru_iter);
        return;
    }

    // Evict LRU entry if cache is full
    evictIfNeeded();

    // Add new entry to front of LRU list
    m_lru_list.push_front(event_id);
    m_cache[event_id] = CacheEntry{entry, m_lru_list.begin()};

    {
        std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
        m_stats.current_size = m_cache.size();
    }
}

void IndexCache::clear() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_cache.clear();
    m_lru_list.clear();

    {
        std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
        m_stats.current_size = 0;
    }
}

IndexCache::Stats IndexCache::getStats() const {
    std::lock_guard<std::mutex> lock(m_stats_mutex);
    return m_stats;
}

void IndexCache::touchEntry(uint64_t event_id, LRUList::iterator lru_iter) {
    // Move to front of LRU list (most recently used)
    m_lru_list.erase(lru_iter);
    m_lru_list.push_front(event_id);
    m_cache[event_id].lru_iter = m_lru_list.begin();
}

void IndexCache::evictIfNeeded() {
    if (m_cache.size() >= m_max_entries) {
        // Evict least recently used (back of list)
        uint64_t lru_event_id = m_lru_list.back();
        m_lru_list.pop_back();
        m_cache.erase(lru_event_id);

        {
            std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
            m_stats.evictions++;
            m_stats.current_size = m_cache.size();
        }
    }
}

}
