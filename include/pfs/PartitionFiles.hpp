#ifndef DIASPORA_PFS_DRIVER_PARTITION_FILES_HPP
#define DIASPORA_PFS_DRIVER_PARTITION_FILES_HPP

#include <pfs/Config.hpp>
#include <string>
#include <string_view>
#include <vector>
#include <mutex>
#include <cstdint>

namespace pfs {

/**
 * Manages the 4 files per partition:
 * - data: Binary file with event data back-to-back
 * - metadata: Binary file with event metadata back-to-back
 * - data-index: Offset/size pairs (64-bit unsigned each)
 * - metadata-index: Offset/size pairs (64-bit unsigned each)
 */
class PartitionFiles {
public:
    struct IndexEntry {
        uint64_t offset;
        uint64_t size;
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
     * @param data Serialized data buffer
     * @return Event ID (0-based index)
     */
    uint64_t appendEvent(const std::vector<char>& metadata,
                         const std::vector<char>& data);

    /**
     * Flush all buffered writes to disk
     */
    void flush();

    /**
     * Read metadata for a specific event
     * @param event_id Event ID (0-based)
     * @return Metadata buffer
     */
    std::vector<char> readMetadata(uint64_t event_id);

    /**
     * Read data for a specific event
     * @param event_id Event ID (0-based)
     * @return Data buffer
     */
    std::vector<char> readData(uint64_t event_id);

    /**
     * Get metadata index entry for a specific event
     * @param event_id Event ID (0-based)
     * @return Index entry with offset and size
     */
    IndexEntry getMetadataIndex(uint64_t event_id);

    /**
     * Get data index entry for a specific event
     * @param event_id Event ID (0-based)
     * @return Index entry with offset and size
     */
    IndexEntry getDataIndex(uint64_t event_id);

    /**
     * Get the number of events in this partition
     * @return Number of events
     */
    uint64_t numEvents() const { return m_num_events; }

private:
    std::string m_base_path;

    // File descriptors
    int m_data_fd;
    int m_metadata_fd;
    int m_data_index_fd;
    int m_metadata_index_fd;

    // Configuration
    bool m_use_locking;
    PfsConfig::FlushBehavior m_flush_behavior;

    // Cached state (protected by mutex)
    mutable std::mutex m_write_mutex;
    uint64_t m_data_offset;
    uint64_t m_metadata_offset;
    uint64_t m_num_events;

    /**
     * Open or create the 4 partition files
     */
    void openOrCreateFiles();

    /**
     * Load index summary (count of events) from index files
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
     * Write an index entry
     */
    void writeIndexEntry(int index_fd, uint64_t offset, uint64_t size);

    /**
     * Read an index entry
     */
    IndexEntry readIndexEntry(int index_fd, uint64_t event_id);

    /**
     * Get file size
     */
    uint64_t getFileSize(int fd);
};

}

#endif
