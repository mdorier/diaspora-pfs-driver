#ifndef DIASPORA_PFS_DRIVER_CONSUMER_HPP
#define DIASPORA_PFS_DRIVER_CONSUMER_HPP

#include <pfs/ThreadPool.hpp>
#include <pfs/TopicHandle.hpp>

#include <diaspora/Consumer.hpp>

namespace pfs {

class PfsConsumer final : public diaspora::ConsumerInterface {

    const std::string                     m_name;
    const diaspora::BatchSize             m_batch_size;
    const diaspora::MaxNumBatches         m_max_num_batches;
    const std::shared_ptr<PfsThreadPool>  m_thread_pool;
    const std::shared_ptr<PfsTopicHandle> m_topic;
    const diaspora::DataAllocator         m_data_allocator;
    const diaspora::DataSelector          m_data_selector;

    std::vector<size_t>                   m_partition_offsets;
    size_t                                m_current_partition = 0;

    // Prefetching state for sequential access optimization
    std::vector<size_t>                   m_prefetch_positions;  // Last prefetched position per partition
    static constexpr size_t               PREFETCH_WINDOW = 32;  // Prefetch 32 events ahead

    public:

    PfsConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<PfsThreadPool> thread_pool,
        std::shared_ptr<PfsTopicHandle> topic,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector);

    const std::string& name() const override {
        return m_name;
    }

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_num_batches;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    const diaspora::DataAllocator& dataAllocator() const override {
        return m_data_allocator;
    }

    const diaspora::DataSelector& dataSelector() const override {
        return m_data_selector;
    }

    void process(diaspora::EventProcessor processor,
                 int timeout_ms,
                 diaspora::NumEvents maxEvents,
                 std::shared_ptr<diaspora::ThreadPoolInterface> threadPool) override;

    void unsubscribe() override;

    diaspora::Future<std::optional<diaspora::Event>> pull() override;

};

}

#endif
