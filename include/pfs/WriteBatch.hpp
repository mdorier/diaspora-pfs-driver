#ifndef DIASPORA_PFS_DRIVER_WRITE_BATCH_HPP
#define DIASPORA_PFS_DRIVER_WRITE_BATCH_HPP

#include <diaspora/DataView.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/BufferWrapperArchive.hpp>
#include <vector>
#include <cstdint>

namespace pfs {

/**
 * Container for batched write events.
 * Stores metadata and data for multiple events before flushing to disk.
 * Uses a compact representation with separate vectors for better cache locality.
 */
class WriteBatch {
public:
    /**
     * Add an event to the batch
     * Serializes metadata directly into the batch buffer and stores DataView without copying
     */
    void addEvent(const diaspora::Metadata& metadata,
                  const diaspora::Serializer& serializer,
                  diaspora::DataView data) {
        // Record the starting size to calculate how much was serialized
        size_t old_size = all_metadata.size();

        // Serialize metadata directly into all_metadata buffer
        diaspora::BufferWrapperOutputArchive archive(all_metadata);
        serializer.serialize(archive, metadata);

        // Calculate how much metadata was added
        size_t metadata_size = all_metadata.size() - old_size;
        metadata_sizes.push_back(metadata_size);
        total_metadata_bytes += metadata_size;

        // Calculate total data size from segments
        auto segments = data.segments();
        for (const auto& seg : segments) {
            total_data_bytes += seg.size;
        }

        // Store DataView directly (no copying)
        data_views.push_back(std::move(data));
    }

    /**
     * Get number of events in batch
     */
    size_t size() const {
        return metadata_sizes.size();
    }

    /**
     * Check if batch is empty
     */
    bool empty() const {
        return metadata_sizes.empty();
    }

    /**
     * Clear all events and reset counters
     */
    void clear() {
        all_metadata.clear();
        metadata_sizes.clear();
        data_views.clear();
        total_metadata_bytes = 0;
        total_data_bytes = 0;
    }

    /**
     * Get total size of all data (metadata + data + index)
     */
    size_t totalBytes() const {
        return total_metadata_bytes + total_data_bytes + (metadata_sizes.size() * 32);
    }

    // Public for PartitionFiles to iterate
    std::vector<char> all_metadata;           // All metadata concatenated back-to-back
    std::vector<size_t> metadata_sizes;       // Size of each metadata entry
    std::vector<diaspora::DataView> data_views; // One DataView per event
    size_t total_metadata_bytes = 0;
    size_t total_data_bytes = 0;
};

}

#endif
