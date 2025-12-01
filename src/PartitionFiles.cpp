#include "pfs/PartitionFiles.hpp"
#include <diaspora/Exception.hpp>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/stat.h>
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
, m_data_index_fd(-1)
, m_metadata_index_fd(-1)
, m_use_locking(use_locking)
, m_flush_behavior(flush_behavior)
, m_data_offset(0)
, m_metadata_offset(0)
, m_num_events(0)
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
, m_data_index_fd(other.m_data_index_fd)
, m_metadata_index_fd(other.m_metadata_index_fd)
, m_use_locking(other.m_use_locking)
, m_flush_behavior(other.m_flush_behavior)
, m_data_offset(other.m_data_offset)
, m_metadata_offset(other.m_metadata_offset)
, m_num_events(other.m_num_events)
{
    other.m_data_fd = -1;
    other.m_metadata_fd = -1;
    other.m_data_index_fd = -1;
    other.m_metadata_index_fd = -1;
}

PartitionFiles& PartitionFiles::operator=(PartitionFiles&& other) noexcept {
    if (this != &other) {
        closeFiles();

        m_base_path = std::move(other.m_base_path);
        m_data_fd = other.m_data_fd;
        m_metadata_fd = other.m_metadata_fd;
        m_data_index_fd = other.m_data_index_fd;
        m_metadata_index_fd = other.m_metadata_index_fd;
        m_use_locking = other.m_use_locking;
        m_flush_behavior = other.m_flush_behavior;
        m_data_offset = other.m_data_offset;
        m_metadata_offset = other.m_metadata_offset;
        m_num_events = other.m_num_events;

        other.m_data_fd = -1;
        other.m_metadata_fd = -1;
        other.m_data_index_fd = -1;
        other.m_metadata_index_fd = -1;
    }
    return *this;
}

void PartitionFiles::openOrCreateFiles() {
    std::string data_path = m_base_path + "/data";
    std::string metadata_path = m_base_path + "/metadata";
    std::string data_index_path = m_base_path + "/data-index";
    std::string metadata_index_path = m_base_path + "/metadata-index";

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

    m_metadata_fd = open(metadata_path.c_str(), flags, mode);
    if (m_metadata_fd == -1) {
        close(m_data_fd);
        throw diaspora::Exception{
            "Failed to open metadata file at " + metadata_path +
            ": " + std::string(strerror(errno))
        };
    }

    m_data_index_fd = open(data_index_path.c_str(), flags, mode);
    if (m_data_index_fd == -1) {
        close(m_data_fd);
        close(m_metadata_fd);
        throw diaspora::Exception{
            "Failed to open data-index file at " + data_index_path +
            ": " + std::string(strerror(errno))
        };
    }

    m_metadata_index_fd = open(metadata_index_path.c_str(), flags, mode);
    if (m_metadata_index_fd == -1) {
        close(m_data_fd);
        close(m_metadata_fd);
        close(m_data_index_fd);
        throw diaspora::Exception{
            "Failed to open metadata-index file at " + metadata_index_path +
            ": " + std::string(strerror(errno))
        };
    }
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
    if (m_data_index_fd != -1) {
        close(m_data_index_fd);
        m_data_index_fd = -1;
    }
    if (m_metadata_index_fd != -1) {
        close(m_metadata_index_fd);
        m_metadata_index_fd = -1;
    }
}

void PartitionFiles::loadIndexSummary() {
    // Get the size of the metadata-index file to determine number of events
    struct stat st;
    if (fstat(m_metadata_index_fd, &st) == -1) {
        throw diaspora::Exception{
            "Failed to stat metadata-index file: " + std::string(strerror(errno))
        };
    }

    // Each index entry is 16 bytes (8 bytes offset + 8 bytes size)
    m_num_events = st.st_size / 16;

    // Get current offsets by seeking to the end of data files
    off_t data_off = lseek(m_data_fd, 0, SEEK_END);
    if (data_off == static_cast<off_t>(-1)) {
        throw diaspora::Exception{
            "Failed to seek data file: " + std::string(strerror(errno))
        };
    }
    m_data_offset = static_cast<uint64_t>(data_off);

    off_t metadata_off = lseek(m_metadata_fd, 0, SEEK_END);
    if (metadata_off == static_cast<off_t>(-1)) {
        throw diaspora::Exception{
            "Failed to seek metadata file: " + std::string(strerror(errno))
        };
    }
    m_metadata_offset = static_cast<uint64_t>(metadata_off);
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
        if (fsync(m_data_index_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync data-index file: " + std::string(strerror(errno))
            };
        }
        if (fsync(m_metadata_index_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync metadata-index file: " + std::string(strerror(errno))
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
        if (fsync(m_data_index_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync data-index file: " + std::string(strerror(errno))
            };
        }
        if (fsync(m_metadata_index_fd) == -1) {
            throw diaspora::Exception{
                "Failed to fsync metadata-index file: " + std::string(strerror(errno))
            };
        }
    }
}

void PartitionFiles::writeIndexEntry(int index_fd, uint64_t offset, uint64_t size) {
    uint64_t entry[2] = {offset, size};
    ssize_t written = write(index_fd, entry, sizeof(entry));
    if (written != sizeof(entry)) {
        throw diaspora::Exception{
            "Failed to write index entry: wrote " + std::to_string(written) +
            " bytes, expected " + std::to_string(sizeof(entry)) +
            " (errno: " + std::to_string(errno) + ")"
        };
    }
}

PartitionFiles::IndexEntry PartitionFiles::readIndexEntry(int index_fd, uint64_t event_id) {
    // Seek to the index entry
    off_t offset = lseek(index_fd, event_id * 16, SEEK_SET);
    if (offset == static_cast<off_t>(-1)) {
        throw diaspora::Exception{
            "Failed to seek in index file: " + std::string(strerror(errno))
        };
    }

    // Read the entry
    uint64_t entry[2];
    ssize_t bytes_read = read(index_fd, entry, sizeof(entry));
    if (bytes_read != sizeof(entry)) {
        throw diaspora::Exception{
            "Failed to read index entry: read " + std::to_string(bytes_read) +
            " bytes, expected " + std::to_string(sizeof(entry))
        };
    }

    return IndexEntry{entry[0], entry[1]};
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
                                      const std::vector<char>& data) {
    std::lock_guard<std::mutex> lock(m_write_mutex);

    // Lock files if needed
    lockFile(m_data_fd);
    lockFile(m_metadata_fd);
    lockFile(m_data_index_fd);
    lockFile(m_metadata_index_fd);

    try {
        // Write metadata to metadata file
        uint64_t metadata_offset = m_metadata_offset;
        ssize_t metadata_written = write(m_metadata_fd, metadata.data(), metadata.size());
        if (metadata_written != static_cast<ssize_t>(metadata.size())) {
            throw diaspora::Exception{
                "Failed to write metadata: wrote " + std::to_string(metadata_written) +
                " bytes, expected " + std::to_string(metadata.size()) +
                " (errno: " + std::to_string(errno) + ")"
            };
        }

        // Write data to data file
        uint64_t data_offset = m_data_offset;
        ssize_t data_written = write(m_data_fd, data.data(), data.size());
        if (data_written != static_cast<ssize_t>(data.size())) {
            throw diaspora::Exception{
                "Failed to write data: wrote " + std::to_string(data_written) +
                " bytes, expected " + std::to_string(data.size()) +
                " (errno: " + std::to_string(errno) + ")"
            };
        }

        // Write metadata index entry
        writeIndexEntry(m_metadata_index_fd, metadata_offset, metadata.size());

        // Write data index entry
        writeIndexEntry(m_data_index_fd, data_offset, data.size());

        // Update offsets
        m_metadata_offset += metadata.size();
        m_data_offset += data.size();

        // Get event ID before incrementing
        uint64_t event_id = m_num_events;
        m_num_events++;

        // Flush if needed
        flushIfNeeded();

        // Unlock files
        unlockFile(m_metadata_index_fd);
        unlockFile(m_data_index_fd);
        unlockFile(m_metadata_fd);
        unlockFile(m_data_fd);

        return event_id;

    } catch (...) {
        // Unlock files on error
        unlockFile(m_metadata_index_fd);
        unlockFile(m_data_index_fd);
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
    auto index_entry = readIndexEntry(m_metadata_index_fd, event_id);

    // Verify index entry is valid
    uint64_t file_size = getFileSize(m_metadata_fd);
    if (index_entry.offset + index_entry.size > file_size) {
        throw diaspora::Exception{
            "Corrupted metadata index: entry points beyond end of file"
        };
    }

    // Seek to metadata position
    off_t offset = lseek(m_metadata_fd, index_entry.offset, SEEK_SET);
    if (offset == static_cast<off_t>(-1)) {
        throw diaspora::Exception{
            "Failed to seek in metadata file: " + std::string(strerror(errno))
        };
    }

    // Read metadata
    std::vector<char> metadata(index_entry.size);
    ssize_t bytes_read = read(m_metadata_fd, metadata.data(), metadata.size());
    if (bytes_read != static_cast<ssize_t>(metadata.size())) {
        throw diaspora::Exception{
            "Failed to read metadata: read " + std::to_string(bytes_read) +
            " bytes, expected " + std::to_string(metadata.size())
        };
    }

    return metadata;
}

std::vector<char> PartitionFiles::readData(uint64_t event_id) {
    if (event_id >= m_num_events) {
        throw diaspora::Exception{
            "Invalid event_id: " + std::to_string(event_id) +
            ", num_events: " + std::to_string(m_num_events)
        };
    }

    // Read index entry
    auto index_entry = readIndexEntry(m_data_index_fd, event_id);

    // Verify index entry is valid
    uint64_t file_size = getFileSize(m_data_fd);
    if (index_entry.offset + index_entry.size > file_size) {
        throw diaspora::Exception{
            "Corrupted data index: entry points beyond end of file"
        };
    }

    // Seek to data position
    off_t offset = lseek(m_data_fd, index_entry.offset, SEEK_SET);
    if (offset == static_cast<off_t>(-1)) {
        throw diaspora::Exception{
            "Failed to seek in data file: " + std::string(strerror(errno))
        };
    }

    // Read data
    std::vector<char> data(index_entry.size);
    ssize_t bytes_read = read(m_data_fd, data.data(), data.size());
    if (bytes_read != static_cast<ssize_t>(data.size())) {
        throw diaspora::Exception{
            "Failed to read data: read " + std::to_string(bytes_read) +
            " bytes, expected " + std::to_string(data.size())
        };
    }

    return data;
}

PartitionFiles::IndexEntry PartitionFiles::getMetadataIndex(uint64_t event_id) {
    if (event_id >= m_num_events) {
        throw diaspora::Exception{
            "Invalid event_id: " + std::to_string(event_id) +
            ", num_events: " + std::to_string(m_num_events)
        };
    }
    return readIndexEntry(m_metadata_index_fd, event_id);
}

PartitionFiles::IndexEntry PartitionFiles::getDataIndex(uint64_t event_id) {
    if (event_id >= m_num_events) {
        throw diaspora::Exception{
            "Invalid event_id: " + std::to_string(event_id) +
            ", num_events: " + std::to_string(m_num_events)
        };
    }
    return readIndexEntry(m_data_index_fd, event_id);
}

}
