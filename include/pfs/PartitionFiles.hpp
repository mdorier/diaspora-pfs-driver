#ifndef DIASPORA_PFS_DRIVER_PARTITION_FILES_HPP
#define DIASPORA_PFS_DRIVER_PARTITION_FILES_HPP

#include <pfs/Config.hpp>
#include <pfs/BufferPool.hpp>
#include <diaspora/DataView.hpp>
#include <diaspora/DataDescriptor.hpp>
#include <string>
#include <string_view>
#include <vector>
#include <mutex>
#include <cstdint>

namespace pfs {

// Forward declaration to avoid circular dependency
class IndexCache;

/**
 * Manages the 3 files per partition:
 * - data: Binary file with event data back-to-back
 * - metadata: Binary file with event metadata back-to-back
 * - index: Combined index with 4 uint64_t values per event:
 *          (metadata_offset, metadata_size, data_offset, data_size)
 */
class PartitionFiles {
public:
    struct IndexEntry {
        uint64_t metadata_offset;
        uint64_t metadata_size;
        uint64_t data_offset;
        uint64_t data_size;
    };

    /**
     * Constructor opens or creates partition files
     * @param base_path Path to partition directory (e.g., /root/topic/partitions/00000000)
     * @param use_locking Whether to use file locking (flock)
     * @param flush_behavior When to flush data to disk
     */
    PartitionFiles(std::string_view base_path,
                   bool use_locking,
                   PfsConfig::FlushBehavior flush_behavior);

    /**
     * Destructor closes all file descriptors
     */
    ~PartitionFiles();

    // Disable copy constructor and assignment
    PartitionFiles(const PartitionFiles&) = delete;
    PartitionFiles& operator=(const PartitionFiles&) = delete;

    // Allow move constructor and assignment
    PartitionFiles(PartitionFiles&&) noexcept;
    PartitionFiles& operator=(PartitionFiles&&) noexcept;

    /**
     * Append an event to the partition
     * @param metadata Serialized metadata buffer
     * @param data DataView containing event data (written directly, zero-copy)
     * @return Event ID (0-based index)
     */
    uint64_t appendEvent(const std::vector<char>& metadata,
                         const diaspora::DataView& data);

    /**
     * Flush all buffered writes (both write batch and fsync) to disk
     */
    void flush();

    /**
     * Flush accumulated write batch to disk using vectored I/O
     */
    void flushBatch();

    /**
     * Read metadata for a specific event
     * @param event_id Event ID (0-based)
     * @return Metadata buffer
     */
    std::vector<char> readMetadata(uint64_t event_id);

    /**
     * Read data for a specific event into a DataView
     * @param event_id Event ID (0-based)
     * @param descriptor DataDescriptor specifying which segments to read
     * @param data_view DataView to write data into (must be pre-allocated with sufficient size)
     */
    void readData(uint64_t event_id,
                  const diaspora::DataDescriptor& descriptor,
                  diaspora::DataView& data_view);

    /**
     * Get index entry for a specific event
     * @param event_id Event ID (0-based)
     * @return Index entry with all offsets and sizes
     */
    IndexEntry getIndexEntry(uint64_t event_id);

    /**
     * Get the number of events in this partition
     * @return Number of events
     */
    uint64_t numEvents() const { return m_num_events; }

    /**
     * Refresh the event count from disk
     * This is useful when multiple processes/objects may be writing to the same partition
     */
    void refreshEventCount();

    /**
     * Bulk read index entries and populate cache (for prefetching)
     * @param start_event_id First event ID to read
     * @param count Number of entries to read
     */
    void prefetchIndexEntries(uint64_t start_event_id, size_t count);

    /**
     * Prefetch data and metadata for upcoming events into page cache
     * Uses readahead() to asynchronously load data, hiding disk latency
     * @param start_event_id First event ID to prefetch
     * @param count Number of events to prefetch
     */
    void prefetchData(uint64_t start_event_id, size_t count);

private:
    std::string m_base_path;

    // File descriptors
    int m_data_fd;
    int m_metadata_fd;
    int m_index_fd;

    // Configuration
    bool m_use_locking;
    PfsConfig::FlushBehavior m_flush_behavior;

    // Cached state (protected by mutex)
    mutable std::mutex m_write_mutex;
    uint64_t m_data_offset;
    uint64_t m_metadata_offset;
    uint64_t m_index_offset;
    uint64_t m_num_events;

    // Cached file sizes (to avoid fstat on every read)
    uint64_t m_cached_data_size;
    uint64_t m_cached_metadata_size;
    uint64_t m_cached_index_size;

    // Buffer pool for reducing allocations
    BufferPool m_buffer_pool;

    // Index cache for reducing index reads
    std::unique_ptr<IndexCache> m_index_cache;

    // Write batching for amortizing syscall overhead
    struct WriteBatch {
        struct Event {
            std::vector<char> metadata;
            std::vector<char> data;  // Copy of actual data bytes
        };
        std::vector<Event> events;
        size_t total_metadata_bytes = 0;
        size_t total_data_bytes = 0;
        size_t total_index_bytes = 0;

        void clear() {
            events.clear();
            total_metadata_bytes = 0;
            total_data_bytes = 0;
            total_index_bytes = 0;
        }

        bool empty() const {
            return events.empty();
        }

        size_t size() const {
            return events.size();
        }
    };

    WriteBatch m_write_batch;
    static constexpr size_t BATCH_SIZE_THRESHOLD = 1024 * 1024;  // 1MB
    static constexpr size_t BATCH_COUNT_THRESHOLD = 100;         // 100 events

    /**
     * Open or create the 3 partition files
     */
    void openOrCreateFiles();

    /**
     * Load index summary (count of events) from index file
     */
    void loadIndexSummary();

    /**
     * Lock a file descriptor (if locking is enabled)
     */
    void lockFile(int fd);

    /**
     * Unlock a file descriptor (if locking is enabled)
     */
    void unlockFile(int fd);

    /**
     * Flush files if needed based on flush behavior
     */
    void flushIfNeeded();

    /**
     * Close all file descriptors
     */
    void closeFiles();

    /**
     * Write an index entry (4 uint64_t values)
     */
    void writeIndexEntry(uint64_t metadata_offset, uint64_t metadata_size,
                         uint64_t data_offset, uint64_t data_size);

    /**
     * Read an index entry (checks cache first)
     */
    IndexEntry readIndexEntry(uint64_t event_id);

    /**
     * Get file size
     */
    uint64_t getFileSize(int fd);
};

}

#endif
