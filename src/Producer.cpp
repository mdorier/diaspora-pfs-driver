#include "FutureState.hpp"
#include "pfs/Producer.hpp"
#include "pfs/TopicHandle.hpp"

namespace pfs {

PfsProducer::PfsProducer(
    std::string name,
    diaspora::BatchSize batch_size,
    diaspora::MaxNumBatches max_num_batches,
    diaspora::Ordering ordering,
    std::shared_ptr<PfsThreadPool> thread_pool,
    std::shared_ptr<PfsTopicHandle> topic)
: m_name{std::move(name)}
, m_batch_size(batch_size)
, m_max_num_batches(max_num_batches)
, m_ordering(ordering)
, m_thread_pool(std::move(thread_pool))
, m_topic(std::move(topic))
, m_partition_batches(m_topic->m_partitions.size())
, m_batch_mutexes(m_topic->m_partitions.size())
{}

PfsProducer::~PfsProducer() {
    try {
        // Flush all pending batches
        for (size_t i = 0; i < m_partition_batches.size(); ++i) {
            std::lock_guard<std::mutex> lock(m_batch_mutexes[i]);
            if (!m_partition_batches[i].empty()) {
                auto& partition = m_topic->getPartition(i);
                partition.writeBatch(std::move(m_partition_batches[i]));
            }
        }
    } catch (...) {
        // Ignore errors in destructor
    }
}

std::shared_ptr<diaspora::TopicHandleInterface> PfsProducer::topic() const {
    return m_topic;
}

diaspora::Future<std::optional<diaspora::Flushed>> PfsProducer::flush() {
    auto state = std::make_shared<FutureState<std::optional<diaspora::Flushed>>>();

    m_thread_pool->pushWork([this, topic=m_topic, state]() {
        try {
            // Flush all pending batches first
            for (size_t i = 0; i < m_partition_batches.size(); ++i) {
                std::lock_guard<std::mutex> lock(m_batch_mutexes[i]);
                if (!m_partition_batches[i].empty()) {
                    topic->getPartition(i).writeBatch(std::move(m_partition_batches[i]));
                    m_partition_batches[i].clear();
                }
            }

            // Then fsync all partitions
            for (size_t i = 0; i < topic->m_partitions.size(); ++i) {
                topic->getPartition(i).flush();
            }

            state->set(diaspora::Flushed{});
        } catch(const diaspora::Exception& ex) {
            state->set(ex);
        }
    });

    return {
        [state](int timeout_ms) { return state->wait(timeout_ms); },
        [state] { return state->test(); }
    };
}

diaspora::Future<std::optional<diaspora::EventID>> PfsProducer::push(
        diaspora::Metadata metadata,
        diaspora::DataView data,
        std::optional<size_t> partition) {
    auto state = std::make_shared<FutureState<std::optional<diaspora::EventID>>>();

    m_thread_pool->pushWork(
        [this, topic=m_topic, state,
         metadata=std::move(metadata),
         data=std::move(data),
         partition]() {
        try {
            // Validation
            topic->validator().validate(metadata, data);

            // Partition selection
            auto partition_index = topic->m_partition_selector.selectPartitionFor(metadata, partition);

            uint64_t event_id;
            {
                std::lock_guard<std::mutex> lock(m_batch_mutexes[partition_index]);

                // Calculate what the event ID will be
                auto& partition_files = topic->getPartition(partition_index);
                event_id = partition_files.numEvents() + m_partition_batches[partition_index].size();

                // Add to batch (WriteBatch serializes metadata and stores DataView directly)
                m_partition_batches[partition_index].addEvent(
                    metadata,
                    topic->serializer(),
                    data
                );

                // Flush if batch is full
                if (m_partition_batches[partition_index].size() >= m_batch_size.value) {
                    partition_files.writeBatch(std::move(m_partition_batches[partition_index]));
                    m_partition_batches[partition_index].clear();
                }
            }

            state->set(event_id);

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
