#include "FutureState.hpp"
#include "pfs/Producer.hpp"
#include "pfs/TopicHandle.hpp"
#include <diaspora/BufferWrapperArchive.hpp>

namespace pfs {

std::shared_ptr<diaspora::TopicHandleInterface> PfsProducer::topic() const {
    return m_topic;
}

diaspora::Future<std::optional<diaspora::Flushed>> PfsProducer::flush() {
    auto state = std::make_shared<FutureState<std::optional<diaspora::Flushed>>>();
    m_thread_pool->pushWork([topic=m_topic, state]() {
        try {
            // Flush all partitions
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
            [topic=m_topic, state,
             metadata=std::move(metadata),
             data=std::move(data),
             partition]() {
            try {
            // validation
            topic->validator().validate(metadata, data);
            // serialization
            std::vector<char> metadata_buffer;
            diaspora::BufferWrapperOutputArchive archive(metadata_buffer);
            topic->serializer().serialize(archive, metadata);
            // partition selection
            auto partition_index = topic->m_partition_selector.selectPartitionFor(metadata, partition);
            // Write to file (zero-copy for data)
            auto& partition_files = topic->getPartition(partition_index);
            auto event_id = partition_files.appendEvent(metadata_buffer, data);
            // set the ID
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
