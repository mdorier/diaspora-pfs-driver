#ifndef DIASPORA_PFS_DRIVER_BUFFER_POOL_HPP
#define DIASPORA_PFS_DRIVER_BUFFER_POOL_HPP

#include <vector>
#include <memory>
#include <mutex>
#include <array>
#include <cstdint>

namespace pfs {

/**
 * Thread-safe buffer pool for reusing memory allocations.
 * Uses size classes to reduce fragmentation: 4KB, 64KB, 1MB, 16MB
 */
class BufferPool {
public:
    /**
     * A reusable buffer from the pool
     */
    struct Buffer {
        std::vector<char> data;
        size_t capacity;

        Buffer(size_t cap) : data(), capacity(cap) {
            data.reserve(cap);
        }
    };

    /**
     * Constructor
     * @param max_buffers_per_class Maximum number of buffers to cache per size class
     */
    explicit BufferPool(size_t max_buffers_per_class = 16);

    /**
     * Acquire a buffer of at least min_size bytes
     * Returns a buffer from the pool or allocates a new one if pool is empty
     * @param min_size Minimum required buffer size
     * @return Shared pointer to buffer (auto-released back to pool when destroyed)
     */
    std::shared_ptr<Buffer> acquire(size_t min_size);

    /**
     * Get statistics about pool usage
     */
    struct Stats {
        size_t total_acquires = 0;
        size_t total_releases = 0;
        size_t cache_hits = 0;
        size_t cache_misses = 0;
        std::array<size_t, 4> buffers_per_class = {0, 0, 0, 0};
    };

    Stats getStats() const;

private:
    // Size classes: 4KB, 64KB, 1MB, 16MB
    static constexpr size_t SIZE_CLASSES[] = {
        4 * 1024,        // 4KB
        64 * 1024,       // 64KB
        1024 * 1024,     // 1MB
        16 * 1024 * 1024 // 16MB
    };
    static constexpr size_t NUM_SIZE_CLASSES = 4;

    size_t m_max_buffers_per_class;

    // One pool per size class
    std::array<std::vector<std::shared_ptr<Buffer>>, NUM_SIZE_CLASSES> m_pools;
    std::array<std::mutex, NUM_SIZE_CLASSES> m_pool_mutexes;

    // Statistics
    mutable std::mutex m_stats_mutex;
    Stats m_stats;

    /**
     * Find the appropriate size class for a requested size
     * @return Index of size class, or NUM_SIZE_CLASSES if too large
     */
    size_t getSizeClass(size_t size) const;

    /**
     * Release a buffer back to the pool
     */
    void release(size_t size_class_idx, std::shared_ptr<Buffer> buffer);

    /**
     * Custom deleter for shared_ptr that returns buffer to pool
     */
    class BufferDeleter {
        BufferPool* m_pool;
        size_t m_size_class_idx;
    public:
        BufferDeleter(BufferPool* pool, size_t size_class_idx)
            : m_pool(pool), m_size_class_idx(size_class_idx) {}

        void operator()(Buffer* buf) {
            if (m_pool && buf) {
                m_pool->release(m_size_class_idx, std::shared_ptr<Buffer>(buf));
            } else {
                delete buf;
            }
        }
    };
};

}

#endif
