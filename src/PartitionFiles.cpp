#include "pfs/PartitionFiles.hpp"
#include "pfs/IndexCache.hpp"
#include <diaspora/Exception.hpp>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <cstring>
#include <sstream>
#include <iomanip>

namespace pfs {

PartitionFiles::PartitionFiles(std::string_view base_path,
                               bool use_locking,
                               PfsConfig::FlushBehavior flush_behavior)
: m_base_path(base_path)
, m_data_fd(-1)
, m_metadata_fd(-1)
, m_index_fd(-1)
, m_use_locking(use_locking)
, m_flush_behavior(flush_behavior)
, m_data_offset(0)
, m_metadata_offset(0)
, m_index_offset(0)
, m_num_events(0)
, m_cached_data_size(0)
, m_cached_metadata_size(0)
, m_cached_index_size(0)
, m_index_cache(std::make_unique<IndexCache>(1024))  // 1024 entries = 32KB
{
    openOrCreateFiles();
    loadIndexSummary();
}

PartitionFiles::~PartitionFiles() {
    closeFiles();
}

PartitionFiles::PartitionFiles(PartitionFiles&& other) noexcept
: m_base_path(std::move(other.m_base_path))
, m_data_fd(other.m_data_fd)
, m_metadata_fd(other.m_metadata_fd)
, m_index_fd(other.m_index_fd)
, m_use_locking(other.m_use_locking)
, m_flush_behavior(other.m_flush_behavior)
, m_data_offset(other.m_data_offset)
, m_metadata_offset(other.m_metadata_offset)
, m_index_offset(other.m_index_offset)
, m_num_events(other.m_num_events)
, m_cached_data_size(other.m_cached_data_size)
, m_cached_metadata_size(other.m_cached_metadata_size)
, m_cached_index_size(other.m_cached_index_size)
{
    other.m_data_fd = -1;
    other.m_metadata_fd = -1;
    other.m_index_fd = -1;
}

PartitionFiles& PartitionFiles::operator=(PartitionFiles&& other) noexcept {
    if (this != &other) {
        closeFiles();

        m_base_path = std::move(other.m_base_path);
        m_data_fd = other.m_data_fd;
        m_metadata_fd = other.m_metadata_fd;
        m_index_fd = other.m_index_fd;
        m_use_locking = other.m_use_locking;
        m_flush_behavior = other.m_flush_behavior;
        m_data_offset = other.m_data_offset;
        m_metadata_offset = other.m_metadata_offset;
        m_index_offset = other.m_index_offset;
        m_num_events = other.m_num_events;
        m_cached_data_size = other.m_cached_data_size;
        m_cached_metadata_size = other.m_cached_metadata_size;
        m_cached_index_size = other.m_cached_index_size;

        other.m_data_fd = -1;
        other.m_metadata_fd = -1;
        other.m_index_fd = -1;
    }
    return *this;
}

void PartitionFiles::openOrCreateFiles() {
    std::string data_path = m_base_path + "/data";
    std::string metadata_path = m_base_path + "/metadata";
    std::string index_path = m_base_path + "/index";

    // Open or create files with read/write permissions
    int flags = O_RDWR | O_CREAT;
    mode_t mode = 0644;

    m_data_fd = open(data_path.c_str(), flags, mode);
    if (m_data_fd == -1) {
        throw diaspora::Exception{
            "Failed to open data file at " + data_path +
            ": " + std::string(strerror(errno))
        };
    }
    // Hint sequential access pattern for data file (append-only writes, sequential reads)
    posix_fadvise(m_data_fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    m_metadata_fd = open(metadata_path.c_str(), flags, mode);
    if (m_metadata_fd == -1) {
        close(m_data_fd);
        throw diaspora::Exception{
            "Failed to open metadata file at " + metadata_path +
            ": " + std::string(strerror(errno))
        };
    }
    // Hint sequential access pattern for metadata file
    posix_fadvise(m_metadata_fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    m_index_fd = open(index_path.c_str(), flags, mode);
    if (m_index_fd == -1) {
        close(m_data_fd);
        close(m_metadata_fd);
        throw diaspora::Exception{
            "Failed to open index file at " + index_path +
            ": " + std::string(strerror(errno))
        };
    }
    // Hint sequential access pattern for index file
    posix_fadvise(m_index_fd, 0, 0, POSIX_FADV_SEQUENTIAL);
}

void PartitionFiles::closeFiles() {
    if (m_data_fd != -1) {
        close(m_data_fd);
        m_data_fd = -1;
    }
    if (m_metadata_fd != -1) {
        close(m_metadata_fd);
        m_metadata_fd = -1;
    }
    if (m_index_fd != -1) {
        close(m_index_fd);
        m_index_fd = -1;
    }
}

void PartitionFiles::loadIndexSummary() {
    // Get the size of the index file to determine number of events
    struct stat index_st;

    if (fstat(m_index_fd, &index_st) == -1) {
        throw diaspora::Exception{
            "Failed to stat index file: " + std::string(strerror(errno))
        };
    }

    // Each index entry is 32 bytes (4 * 8 bytes for metadata_offset, metadata_size, data_offset, data_size)
    m_num_events = index_st.st_size / 32;
    m_index_offset = static_cast<uint64_t>(index_st.st_size);
    m_cached_index_size = static_cast<uint64_t>(index_st.st_size);

    // Get current offsets by seeking to the end of data files
    off_t data_off = lseek(m_data_fd, 0, SEEK_END);
    if (data_off == static_cast<off_t>(-1)) {
        throw diaspora::Exception{
            "Failed to seek data file: " + std::string(strerror(errno))
        };
    }
    m_data_offset = static_cast<uint64_t>(data_off);
    m_cached_data_size = static_cast<uint64_t>(data_off);

    off_t metadata_off = lseek(m_metadata_fd, 0, SEEK_END);
    if (metadata_off == static_cast<off_t>(-1)) {
        throw diaspora::Exception{
            "Failed to seek metadata file: " + std::string(strerror(errno))
        };
    }
    m_metadata_offset = static_cast<uint64_t>(metadata_off);
    m_cached_metadata_size = static_cast<uint64_t>(metadata_off);
}

void PartitionFiles::lockFile(int fd) {
    if (m_use_locking) {
        if (flock(fd, LOCK_EX) == -1) {
            throw diaspora::Exception{
                "Failed to lock file: " + std::string(strerror(errno))
            };
        }
    }
}

void PartitionFiles::unlockFile(int fd) {
    if (m_use_locking) {
        if (flock(fd, LOCK_UN) == -1) {
            throw diaspora::Exception{
                "Failed to unlock file: " + std::string(strerror(errno))
            };
        }
    }
}

void PartitionFiles::flushIfNeeded() {
    if (m_flush_behavior == PfsConfig::FlushBehavior::IMMEDIATE) {
        if (fsync(m_data_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync data file: " + std::string(strerror(errno))
            };
        }
        if (fsync(m_metadata_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync metadata file: " + std::string(strerror(errno))
            };
        }
        if (fsync(m_index_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync index file: " + std::string(strerror(errno))
            };
        }
    }
}

uint64_t PartitionFiles::writeBatch(WriteBatch&& batch) {
    if (batch.empty()) {
        return m_num_events;  // No events written
    }

    std::lock_guard<std::mutex> lock(m_write_mutex);

    lockFile(m_data_fd);
    lockFile(m_metadata_fd);
    lockFile(m_index_fd);

    try {
        uint64_t first_event_id = m_num_events;

        // Track starting offsets
        uint64_t metadata_start = m_metadata_offset;
        uint64_t data_start = m_data_offset;

        // Build iovec arrays for vectored I/O
        std::vector<struct iovec> metadata_iov;
        std::vector<struct iovec> data_iov;
        std::vector<uint64_t> index_buffer;

        metadata_iov.reserve(batch.metadata_sizes.size());
        index_buffer.reserve(batch.metadata_sizes.size() * 4);

        uint64_t current_metadata_offset = metadata_start;
        uint64_t current_data_offset = data_start;

        // Build iovec arrays by iterating through the batch
        size_t metadata_offset_in_batch = 0;
        for (size_t i = 0; i < batch.metadata_sizes.size(); ++i) {
            size_t metadata_size = batch.metadata_sizes[i];

            // Metadata iovec
            struct iovec meta_vec;
            meta_vec.iov_base = batch.all_metadata.data() + metadata_offset_in_batch;
            meta_vec.iov_len = metadata_size;
            metadata_iov.push_back(meta_vec);

            // Data iovec - get segments from DataView
            auto data_segments = batch.data_views[i].segments();
            size_t total_data_size = 0;
            for (const auto& seg : data_segments) {
                if (seg.size > 0) {
                    struct iovec data_vec;
                    data_vec.iov_base = const_cast<void*>(seg.ptr);
                    data_vec.iov_len = seg.size;
                    data_iov.push_back(data_vec);
                    total_data_size += seg.size;
                }
            }

            // Index entry
            index_buffer.push_back(current_metadata_offset);
            index_buffer.push_back(metadata_size);
            index_buffer.push_back(current_data_offset);
            index_buffer.push_back(total_data_size);

            current_metadata_offset += metadata_size;
            current_data_offset += total_data_size;
            metadata_offset_in_batch += metadata_size;
        }

        // Write all metadata in one vectored I/O call
        if (!metadata_iov.empty()) {
            ssize_t written = pwritev(m_metadata_fd, metadata_iov.data(),
                                     metadata_iov.size(), metadata_start);
            if (written != static_cast<ssize_t>(batch.total_metadata_bytes)) {
                throw diaspora::Exception{
                    "Failed to write batch metadata: wrote " + std::to_string(written) +
                    " bytes, expected " + std::to_string(batch.total_metadata_bytes)
                };
            }
        }

        // Write all data in one vectored I/O call
        if (!data_iov.empty()) {
            ssize_t written = pwritev(m_data_fd, data_iov.data(),
                                     data_iov.size(), data_start);
            if (written != static_cast<ssize_t>(batch.total_data_bytes)) {
                throw diaspora::Exception{
                    "Failed to write batch data: wrote " + std::to_string(written) +
                    " bytes, expected " + std::to_string(batch.total_data_bytes)
                };
            }
        }

        // Write all index entries in one call
        if (!index_buffer.empty()) {
            size_t index_bytes = index_buffer.size() * sizeof(uint64_t);
            ssize_t written = pwrite(m_index_fd, index_buffer.data(),
                                    index_bytes, m_index_offset);
            if (written != static_cast<ssize_t>(index_bytes)) {
                throw diaspora::Exception{
                    "Failed to write batch index: wrote " + std::to_string(written) +
                    " bytes, expected " + std::to_string(index_bytes)
                };
            }
        }

        // Update offsets and counts
        m_metadata_offset += batch.total_metadata_bytes;
        m_data_offset += batch.total_data_bytes;
        m_index_offset += batch.metadata_sizes.size() * 32;
        m_cached_metadata_size = m_metadata_offset;
        m_cached_data_size = m_data_offset;
        m_cached_index_size = m_index_offset;
        m_num_events += batch.metadata_sizes.size();

        flushIfNeeded();

        unlockFile(m_index_fd);
        unlockFile(m_metadata_fd);
        unlockFile(m_data_fd);

        return first_event_id;

    } catch (...) {
        unlockFile(m_index_fd);
        unlockFile(m_metadata_fd);
        unlockFile(m_data_fd);
        throw;
    }
}

void PartitionFiles::flush() {
    std::lock_guard<std::mutex> lock(m_write_mutex);

    if (m_flush_behavior != PfsConfig::FlushBehavior::BUFFERED) {
        if (fsync(m_data_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync data file: " + std::string(strerror(errno))
            };
        }
        if (fsync(m_metadata_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync metadata file: " + std::string(strerror(errno))
            };
        }
        if (fsync(m_index_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync index file: " + std::string(strerror(errno))
            };
        }
    }
}

void PartitionFiles::writeIndexEntry(uint64_t metadata_offset, uint64_t metadata_size,
                                     uint64_t data_offset, uint64_t data_size) {
    uint64_t entry[4] = {metadata_offset, metadata_size, data_offset, data_size};
    ssize_t written = pwrite(m_index_fd, entry, sizeof(entry), m_index_offset);
    if (written != sizeof(entry)) {
        throw diaspora::Exception{
            "Failed to write index entry: wrote " + std::to_string(written) +
            " bytes, expected " + std::to_string(sizeof(entry)) +
            " (errno: " + std::to_string(errno) + ")"
        };
    }
}

PartitionFiles::IndexEntry PartitionFiles::readIndexEntry(uint64_t event_id) {
    // Check cache first
    auto cached = m_index_cache->get(event_id);
    if (cached.has_value()) {
        return cached.value();  // Cache hit!
    }

    // Cache miss - read from disk
    // Verify the index file is large enough for this entry (using cached size)
    uint64_t required_size = (event_id + 1) * 32;
    if (m_cached_index_size < required_size) {
        throw diaspora::Exception{
            "Index file corrupted or incomplete: size is " + std::to_string(m_cached_index_size) +
            " bytes, but need at least " + std::to_string(required_size) +
            " bytes to read event " + std::to_string(event_id)
        };
    }

    // Read the index entry using pread (atomic, no lseek needed)
    uint64_t entry[4];
    ssize_t bytes_read = pread(m_index_fd, entry, sizeof(entry), event_id * 32);
    if (bytes_read != sizeof(entry)) {
        throw diaspora::Exception{
            "Failed to read index entry: read " + std::to_string(bytes_read) +
            " bytes, expected " + std::to_string(sizeof(entry))
        };
    }

    IndexEntry result{entry[0], entry[1], entry[2], entry[3]};

    // Cache the entry for future reads
    m_index_cache->put(event_id, result);

    return result;
}

void PartitionFiles::prefetchIndexEntries(uint64_t start_event_id, size_t count) {
    if (count == 0 || start_event_id >= m_num_events) {
        return;  // Nothing to prefetch
    }

    // Limit count to available events
    count = std::min(count, static_cast<size_t>(m_num_events - start_event_id));

    // Calculate how many bytes to read (32 bytes per entry)
    size_t bytes_to_read = count * 32;
    uint64_t disk_offset = start_event_id * 32;

    // Verify we won't read beyond the index file
    if (disk_offset + bytes_to_read > m_cached_index_size) {
        return;  // Don't prefetch beyond file bounds
    }

    // Allocate buffer for bulk read
    std::vector<uint64_t> buffer(count * 4);  // 4 uint64_t per entry

    // Bulk read all entries in one syscall
    ssize_t bytes_read = pread(m_index_fd, buffer.data(), bytes_to_read, disk_offset);
    if (bytes_read != static_cast<ssize_t>(bytes_to_read)) {
        // Prefetch failed, but don't throw - just skip it
        return;
    }

    // Populate cache with all read entries
    for (size_t i = 0; i < count; ++i) {
        uint64_t event_id = start_event_id + i;
        size_t offset = i * 4;

        IndexEntry entry{
            buffer[offset + 0],  // metadata_offset
            buffer[offset + 1],  // metadata_size
            buffer[offset + 2],  // data_offset
            buffer[offset + 3]   // data_size
        };

        m_index_cache->put(event_id, entry);
    }
}

void PartitionFiles::prefetchData(uint64_t start_event_id, size_t count) {
    if (count == 0 || start_event_id >= m_num_events) {
        return;  // Nothing to prefetch
    }

    // Limit count to available events
    count = std::min(count, static_cast<size_t>(m_num_events - start_event_id));

    // First, prefetch index entries if not already cached
    prefetchIndexEntries(start_event_id, count);

    // Now prefetch the actual data and metadata for each event
    for (size_t i = 0; i < count; ++i) {
        uint64_t event_id = start_event_id + i;

        // Get index entry (should be cached from prefetchIndexEntries above)
        auto entry = readIndexEntry(event_id);

        // Prefetch metadata file region
        // readahead() is a Linux-specific syscall that asynchronously loads data into page cache
        #ifdef __linux__
        readahead(m_metadata_fd, entry.metadata_offset, entry.metadata_size);
        readahead(m_data_fd, entry.data_offset, entry.data_size);
        #else
        // Fallback: use posix_fadvise on non-Linux systems
        posix_fadvise(m_metadata_fd, entry.metadata_offset, entry.metadata_size, POSIX_FADV_WILLNEED);
        posix_fadvise(m_data_fd, entry.data_offset, entry.data_size, POSIX_FADV_WILLNEED);
        #endif
    }
}

uint64_t PartitionFiles::getFileSize(int fd) {
    struct stat st;
    if (fstat(fd, &st) == -1) {
        throw diaspora::Exception{
            "Failed to stat file: " + std::string(strerror(errno))
        };
    }
    return static_cast<uint64_t>(st.st_size);
}

uint64_t PartitionFiles::appendEvent(const std::vector<char>& metadata,
                                      const diaspora::DataView& data) {
    std::lock_guard<std::mutex> lock(m_write_mutex);

    // Lock files if needed
    lockFile(m_data_fd);
    lockFile(m_metadata_fd);
    lockFile(m_index_fd);

    try {
        // Current offset becomes the event ID
        uint64_t event_id = m_num_events;

        // Write metadata
        uint64_t metadata_offset = m_metadata_offset;
        ssize_t metadata_written = pwrite(m_metadata_fd, metadata.data(),
                                         metadata.size(), metadata_offset);
        if (metadata_written != static_cast<ssize_t>(metadata.size())) {
            throw diaspora::Exception{
                "Failed to write metadata: wrote " + std::to_string(metadata_written) +
                " bytes, expected " + std::to_string(metadata.size())
            };
        }

        // Write data (handle DataView segments)
        uint64_t data_offset = m_data_offset;
        auto segments = data.segments();
        size_t total_data_size = 0;

        for (const auto& seg : segments) {
            total_data_size += seg.size;
        }

        ssize_t data_written = 0;
        if (segments.size() == 0) {
            // No data to write
            data_written = 0;
        } else if (segments.size() == 1) {
            // Single segment - use pwrite
            data_written = pwrite(m_data_fd, segments[0].ptr, segments[0].size, data_offset);
        } else {
            // Multiple segments - use pwritev for efficiency
            std::vector<struct iovec> iov(segments.size());
            for (size_t i = 0; i < segments.size(); ++i) {
                iov[i].iov_base = const_cast<void*>(segments[i].ptr);
                iov[i].iov_len = segments[i].size;
            }
            data_written = pwritev(m_data_fd, iov.data(), segments.size(), data_offset);
        }

        if (data_written != static_cast<ssize_t>(total_data_size)) {
            throw diaspora::Exception{
                "Failed to write data: wrote " + std::to_string(data_written) +
                " bytes, expected " + std::to_string(total_data_size)
            };
        }

        // Write index entry
        writeIndexEntry(metadata_offset, metadata.size(), data_offset, total_data_size);

        // Update offsets and counts
        m_metadata_offset += metadata.size();
        m_data_offset += total_data_size;
        m_index_offset += 32;
        m_cached_metadata_size = m_metadata_offset;
        m_cached_data_size = m_data_offset;
        m_cached_index_size = m_index_offset;
        m_num_events++;

        flushIfNeeded();

        unlockFile(m_index_fd);
        unlockFile(m_metadata_fd);
        unlockFile(m_data_fd);

        return event_id;

    } catch (...) {
        unlockFile(m_index_fd);
        unlockFile(m_metadata_fd);
        unlockFile(m_data_fd);
        throw;
    }
}

std::vector<char> PartitionFiles::readMetadata(uint64_t event_id) {
    if (event_id >= m_num_events) {
        throw diaspora::Exception{
            "Invalid event_id: " + std::to_string(event_id) +
            ", num_events: " + std::to_string(m_num_events)
        };
    }

    // Read index entry
    auto index_entry = readIndexEntry(event_id);

    // Verify index entry is valid (using cached size)
    if (index_entry.metadata_offset + index_entry.metadata_size > m_cached_metadata_size) {
        throw diaspora::Exception{
            "Corrupted metadata index: entry points beyond end of file"
        };
    }

    // Read metadata using pread (atomic, no lseek needed) with pooled buffer
    auto buffer = m_buffer_pool.acquire(index_entry.metadata_size);
    buffer->data.resize(index_entry.metadata_size);
    ssize_t bytes_read = pread(m_metadata_fd, buffer->data.data(), buffer->data.size(), index_entry.metadata_offset);
    if (bytes_read != static_cast<ssize_t>(buffer->data.size())) {
        throw diaspora::Exception{
            "Failed to read metadata: read " + std::to_string(bytes_read) +
            " bytes, expected " + std::to_string(buffer->data.size())
        };
    }

    return buffer->data;
}

void PartitionFiles::readData(uint64_t event_id,
                              const diaspora::DataDescriptor& descriptor,
                              diaspora::DataView& data_view) {
    if (event_id >= m_num_events) {
        throw diaspora::Exception{
            "Invalid event_id: " + std::to_string(event_id) +
            ", num_events: " + std::to_string(m_num_events)
        };
    }

    // Read index entry to know where the full data is stored on disk
    auto index_entry = readIndexEntry(event_id);

    // Verify index entry is valid (using cached size)
    if (index_entry.data_offset + index_entry.data_size > m_cached_data_size) {
        throw diaspora::Exception{
            "Corrupted data index: entry points beyond end of file"
        };
    }

    // Flatten the descriptor to get segments we need to read from disk
    auto desc_segments = descriptor.flatten();

    // Get DataView's memory segments where we'll write the data
    auto view_segments = data_view.segments();

    // Track position in DataView segments as we fill them
    size_t view_seg_idx = 0;
    size_t view_seg_offset = 0;

    // Read each descriptor segment directly into DataView segments using preadv
    for (const auto& desc_seg : desc_segments) {
        // Verify segment is within the bounds of the stored data
        if (desc_seg.offset + desc_seg.size > index_entry.data_size) {
            throw diaspora::Exception{
                "Requested segment [" + std::to_string(desc_seg.offset) + ", " +
                std::to_string(desc_seg.offset + desc_seg.size) + ") exceeds data size " +
                std::to_string(index_entry.data_size)
            };
        }

        uint64_t disk_offset = index_entry.data_offset + desc_seg.offset;
        size_t remaining = desc_seg.size;

        // Build iovec array mapping this descriptor segment to DataView segment(s)
        std::vector<struct iovec> iov;

        while (remaining > 0) {
            if (view_seg_idx >= view_segments.size()) {
                throw diaspora::Exception{
                    "DataView has insufficient capacity for descriptor segments"
                };
            }

            auto& view_seg = view_segments[view_seg_idx];
            size_t available = view_seg.size - view_seg_offset;
            size_t chunk = std::min(remaining, available);

            // Add this chunk to iovec for preadv
            struct iovec vec;
            vec.iov_base = static_cast<char*>(const_cast<void*>(view_seg.ptr)) + view_seg_offset;
            vec.iov_len = chunk;
            iov.push_back(vec);

            remaining -= chunk;
            view_seg_offset += chunk;

            // Move to next DataView segment if current one is full
            if (view_seg_offset >= view_seg.size) {
                view_seg_idx++;
                view_seg_offset = 0;
            }
        }

        // Read from disk directly into DataView segments (true zero-copy!)
        ssize_t bytes_read;
        if (iov.size() == 1) {
            // Single segment - use pread
            bytes_read = pread(m_data_fd, iov[0].iov_base, iov[0].iov_len, disk_offset);
        } else {
            // Multiple segments - use preadv for vectored I/O
            bytes_read = preadv(m_data_fd, iov.data(), iov.size(), disk_offset);
        }

        if (bytes_read != static_cast<ssize_t>(desc_seg.size)) {
            throw diaspora::Exception{
                "Failed to read data segment: read " + std::to_string(bytes_read) +
                " bytes, expected " + std::to_string(desc_seg.size)
            };
        }
    }
}

PartitionFiles::IndexEntry PartitionFiles::getIndexEntry(uint64_t event_id) {
    if (event_id >= m_num_events) {
        throw diaspora::Exception{
            "Invalid event_id: " + std::to_string(event_id) +
            ", num_events: " + std::to_string(m_num_events)
        };
    }
    return readIndexEntry(event_id);
}

void PartitionFiles::refreshEventCount() {
    struct stat index_st;
    if (fstat(m_index_fd, &index_st) == -1) {
        throw diaspora::Exception{
            "Failed to stat index file: " + std::string(strerror(errno))
        };
    }
    m_num_events = index_st.st_size / 32;
    m_index_offset = static_cast<uint64_t>(index_st.st_size);
    m_cached_index_size = static_cast<uint64_t>(index_st.st_size);
}

}
