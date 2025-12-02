#include "pfs/Consumer.hpp"
#include "pfs/Driver.hpp"
#include "pfs/TopicHandle.hpp"
#include "pfs/ThreadPool.hpp"
#include "pfs/Event.hpp"
#include "FutureState.hpp"

#include <diaspora/BufferWrapperArchive.hpp>
#include <condition_variable>
#include <cstring>
#include <algorithm>

namespace pfs {

PfsConsumer::PfsConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<PfsThreadPool> thread_pool,
        std::shared_ptr<PfsTopicHandle> topic,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector)
: m_name{std::move(name)}
, m_batch_size(batch_size)
, m_max_num_batches(max_num_batches)
, m_thread_pool(std::move(thread_pool))
, m_topic(std::move(topic))
, m_data_allocator{std::move(data_allocator)}
, m_data_selector{std::move(data_selector)}
, m_partition_offsets(m_topic->m_partitions.size(), 0)
, m_prefetch_positions(m_topic->m_partitions.size(), 0)
{}

std::shared_ptr<diaspora::TopicHandleInterface> PfsConsumer::topic() const {
      return m_topic;
}

void PfsConsumer::unsubscribe() {}

void PfsConsumer::process(
        diaspora::EventProcessor processor,
        int timeout_ms,
        diaspora::NumEvents maxEvents,
        std::shared_ptr<diaspora::ThreadPoolInterface> threadPool) {
    if(!threadPool) threadPool = m_topic->driver()->defaultThreadPool();
    size_t                  pending_events = 0;
    std::mutex              pending_mutex;
    std::condition_variable pending_cv;
    try {
        for(size_t i = 0; i < maxEvents.value; ++i) {
            auto event = pull().wait(timeout_ms);
            {
                std::unique_lock lock{pending_mutex};
                pending_events += 1;
            }
            threadPool->pushWork([&, event=std::move(event)]() {
                    processor(event);
                    std::unique_lock lock{pending_mutex};
                    pending_events -= 1;
                    if(pending_events == 0)
                    pending_cv.notify_all();
                    });
        }
    } catch(const diaspora::StopEventProcessor&) {}
    std::unique_lock lock{pending_mutex};
    while(pending_events) pending_cv.wait(lock);
}

diaspora::Future<std::optional<diaspora::Event>> PfsConsumer::pull() {
    auto state = std::make_shared<FutureState<std::optional<diaspora::Event>>>();

    m_thread_pool->pushWork([this, state]() {
        try {
            // Try each partition in round-robin order
            for (size_t i = 0; i < m_topic->m_partitions.size(); ++i) {
                size_t partition_idx = (m_current_partition + i) % m_topic->m_partitions.size();
                auto& partition = m_topic->getPartition(partition_idx);

                // Refresh event count to ensure we have the latest value
                partition.refreshEventCount();

                // Check if this partition has more events
                if (m_partition_offsets[partition_idx] < partition.numEvents()) {
                    // Read metadata from file
                    auto metadata_buffer = partition.readMetadata(m_partition_offsets[partition_idx]);

                    // Deserialize metadata
                    diaspora::Metadata metadata;
                    std::string_view metadata_view(metadata_buffer.data(), metadata_buffer.size());
                    diaspora::BufferWrapperInputArchive archive(metadata_view);
                    m_topic->serializer().deserialize(archive, metadata);

                    // Get data size from index
                    auto index_entry = partition.getIndexEntry(m_partition_offsets[partition_idx]);
                    size_t data_size = index_entry.data_size;

                    // Create data descriptor for the full data
                    diaspora::DataDescriptor full_descriptor("", data_size);

                    // Apply data selector if present
                    diaspora::DataDescriptor selected_descriptor = full_descriptor;
                    if (m_data_selector) {
                        selected_descriptor = m_data_selector(metadata, full_descriptor);
                    }

                    // Allocate data using data allocator
                    diaspora::DataView allocated_view;
                    if(m_data_allocator) {
                        allocated_view = m_data_allocator(metadata, selected_descriptor);

                        // Read selected data segments directly from disk into allocated_view
                        // This is zero-copy: only the requested segments are read from disk
                        partition.readData(m_partition_offsets[partition_idx], selected_descriptor, allocated_view);
                    }

                    // Create event
                    auto event = diaspora::Event(std::make_shared<PfsEvent>(
                        std::move(metadata),
                        allocated_view,
                        m_topic->partitions()[partition_idx],
                        m_partition_offsets[partition_idx]
                    ));

                    // Increment offset for next pull
                    m_partition_offsets[partition_idx]++;
                    m_current_partition = (partition_idx + 1) % m_topic->m_partitions.size();

                    // Trigger prefetching for sequential access optimization
                    // If we've caught up to or passed our prefetch position, prefetch the next window
                    if (m_partition_offsets[partition_idx] >= m_prefetch_positions[partition_idx]) {
                        // Prefetch upcoming events asynchronously
                        size_t prefetch_start = m_partition_offsets[partition_idx];
                        partition.prefetchData(prefetch_start, PREFETCH_WINDOW);
                        m_prefetch_positions[partition_idx] = prefetch_start + PREFETCH_WINDOW;
                    }

                    state->set(event);
                    return;
                }
            }

            // No events available in any partition
            state->set(std::nullopt);

        } catch(const diaspora::Exception& ex) {
            state->set(ex);
        }
    });

    return {
        [state](int timeout_ms) { return state->wait(timeout_ms); },
        [state] { return state->test(); }
    };
}

}
