#include "pfs/BufferPool.hpp"
#include <algorithm>

namespace pfs {

constexpr size_t BufferPool::SIZE_CLASSES[];

BufferPool::BufferPool(size_t max_buffers_per_class)
    : m_max_buffers_per_class(max_buffers_per_class)
{
}

size_t BufferPool::getSizeClass(size_t size) const {
    for (size_t i = 0; i < NUM_SIZE_CLASSES; ++i) {
        if (size <= SIZE_CLASSES[i]) {
            return i;
        }
    }
    // Size is larger than largest class
    return NUM_SIZE_CLASSES;
}

std::shared_ptr<BufferPool::Buffer> BufferPool::acquire(size_t min_size) {
    size_t size_class_idx = getSizeClass(min_size);

    // Update stats
    {
        std::lock_guard<std::mutex> lock(m_stats_mutex);
        m_stats.total_acquires++;
    }

    // If size is too large for pooling, allocate directly
    if (size_class_idx >= NUM_SIZE_CLASSES) {
        std::lock_guard<std::mutex> lock(m_stats_mutex);
        m_stats.cache_misses++;
        return std::make_shared<Buffer>(min_size);
    }

    size_t buffer_size = SIZE_CLASSES[size_class_idx];

    // Try to get buffer from pool
    {
        std::lock_guard<std::mutex> lock(m_pool_mutexes[size_class_idx]);
        if (!m_pools[size_class_idx].empty()) {
            auto buffer = m_pools[size_class_idx].back();
            m_pools[size_class_idx].pop_back();

            // Update stats
            {
                std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
                m_stats.cache_hits++;
                m_stats.buffers_per_class[size_class_idx]--;
            }

            // Clear the buffer and resize for reuse
            buffer->data.clear();
            buffer->data.resize(buffer_size);
            return buffer;
        }
    }

    // Pool is empty, allocate new buffer
    {
        std::lock_guard<std::mutex> lock(m_stats_mutex);
        m_stats.cache_misses++;
    }

    // Create a shared_ptr with custom deleter that returns to pool
    auto buffer = std::shared_ptr<Buffer>(
        new Buffer(buffer_size),
        [this, size_class_idx](Buffer* buf) {
            this->release(size_class_idx, std::shared_ptr<Buffer>(buf, [](Buffer*){}));
        }
    );

    return buffer;
}

void BufferPool::release(size_t size_class_idx, std::shared_ptr<Buffer> buffer) {
    if (!buffer || size_class_idx >= NUM_SIZE_CLASSES) {
        return;
    }

    // Update stats
    {
        std::lock_guard<std::mutex> lock(m_stats_mutex);
        m_stats.total_releases++;
    }

    // Return to pool if not full
    std::lock_guard<std::mutex> lock(m_pool_mutexes[size_class_idx]);
    if (m_pools[size_class_idx].size() < m_max_buffers_per_class) {
        m_pools[size_class_idx].push_back(buffer);

        // Update stats
        {
            std::lock_guard<std::mutex> stats_lock(m_stats_mutex);
            m_stats.buffers_per_class[size_class_idx]++;
        }
    }
    // Otherwise, let it be destroyed
}

BufferPool::Stats BufferPool::getStats() const {
    std::lock_guard<std::mutex> lock(m_stats_mutex);
    return m_stats;
}

}
