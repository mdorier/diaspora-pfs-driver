#include "pfs/PartitionFiles.hpp"
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

    return IndexEntry{entry[0], entry[1], entry[2], entry[3]};
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
        // Write metadata to metadata file using pwrite
        uint64_t metadata_offset = m_metadata_offset;
        ssize_t metadata_written = pwrite(m_metadata_fd, metadata.data(), metadata.size(), metadata_offset);
        if (metadata_written != static_cast<ssize_t>(metadata.size())) {
            throw diaspora::Exception{
                "Failed to write metadata: wrote " + std::to_string(metadata_written) +
                " bytes, expected " + std::to_string(metadata.size()) +
                " (errno: " + std::to_string(errno) + ")"
            };
        }

        // Write data to data file using pwritev (zero-copy for DataView segments)
        uint64_t data_offset = m_data_offset;
        auto segments = data.segments();
        size_t num_segments = segments.size();

        // Calculate total data size
        size_t total_data_size = 0;
        for (const auto& seg : segments) {
            total_data_size += seg.size;
        }

        ssize_t data_written = 0;
        if (num_segments == 0) {
            // No data to write
            data_written = 0;
        } else if (num_segments == 1) {
            // Single segment - use pwrite directly
            const auto& seg = segments[0];
            data_written = pwrite(m_data_fd, seg.ptr, seg.size, data_offset);
        } else {
            // Multiple segments - use pwritev for efficiency
            std::vector<struct iovec> iov(num_segments);
            for (size_t i = 0; i < num_segments; ++i) {
                iov[i].iov_base = const_cast<void*>(segments[i].ptr);
                iov[i].iov_len = segments[i].size;
            }
            data_written = pwritev(m_data_fd, iov.data(), num_segments, data_offset);
        }

        if (data_written != static_cast<ssize_t>(total_data_size)) {
            throw diaspora::Exception{
                "Failed to write data: wrote " + std::to_string(data_written) +
                " bytes, expected " + std::to_string(total_data_size) +
                " (errno: " + std::to_string(errno) + ")"
            };
        }

        // Write combined index entry
        writeIndexEntry(metadata_offset, metadata.size(), data_offset, total_data_size);

        // Update offsets and cached sizes
        m_metadata_offset += metadata.size();
        m_data_offset += total_data_size;
        m_index_offset += 32;
        m_cached_metadata_size = m_metadata_offset;
        m_cached_data_size = m_data_offset;
        m_cached_index_size = m_index_offset;

        // Get event ID before incrementing
        uint64_t event_id = m_num_events;
        m_num_events++;

        // Flush if needed
        flushIfNeeded();

        // Unlock files
        unlockFile(m_index_fd);
        unlockFile(m_metadata_fd);
        unlockFile(m_data_fd);

        return event_id;

    } catch (...) {
        // Unlock files on error
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

    // Flatten the descriptor to get segments we need to read
    auto segments = descriptor.flatten();

    // Read each requested segment directly from disk into the DataView
    size_t data_view_offset = 0;
    for (const auto& segment : segments) {
        // Verify segment is within the bounds of the stored data
        if (segment.offset + segment.size > index_entry.data_size) {
            throw diaspora::Exception{
                "Requested segment [" + std::to_string(segment.offset) + ", " +
                std::to_string(segment.offset + segment.size) + ") exceeds data size " +
                std::to_string(index_entry.data_size)
            };
        }

        // Acquire buffer from pool for this segment
        auto buffer = m_buffer_pool.acquire(segment.size);
        buffer->data.resize(segment.size);

        // Read segment from disk at the appropriate offset
        uint64_t disk_offset = index_entry.data_offset + segment.offset;
        ssize_t bytes_read = pread(m_data_fd, buffer->data.data(), segment.size, disk_offset);
        if (bytes_read != static_cast<ssize_t>(segment.size)) {
            throw diaspora::Exception{
                "Failed to read data segment: read " + std::to_string(bytes_read) +
                " bytes, expected " + std::to_string(segment.size)
            };
        }

        // Write segment into the DataView at the current offset
        data_view.write(buffer->data.data(), segment.size, data_view_offset);
        data_view_offset += segment.size;
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
