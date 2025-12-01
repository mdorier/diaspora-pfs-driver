#ifndef DIASPORA_PFS_DRIVER_PRODUCER_HPP
#define DIASPORA_PFS_DRIVER_PRODUCER_HPP

#include <pfs/ThreadPool.hpp>
#include <pfs/TopicHandle.hpp>
#include <diaspora/Producer.hpp>

namespace diaspora_pfs_driver {

class DiasporaPfsDriverProducer final : public diaspora::ProducerInterface {

    const std::string                     m_name;
    const diaspora::BatchSize             m_batch_size;
    const diaspora::MaxNumBatches         m_max_num_batches;
    const diaspora::Ordering              m_ordering;
    const std::shared_ptr<DiasporaPfsDriverThreadPool>  m_thread_pool;
    const std::shared_ptr<DiasporaPfsDriverTopicHandle> m_topic;

    public:

    DiasporaPfsDriverProducer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        diaspora::Ordering ordering,
        std::shared_ptr<DiasporaPfsDriverThreadPool> thread_pool,
        std::shared_ptr<DiasporaPfsDriverTopicHandle> topic)
    : m_name{std::move(name)}
    , m_batch_size(batch_size)
    , m_max_num_batches(max_num_batches)
    , m_ordering(ordering)
    , m_thread_pool(std::move(thread_pool))
    , m_topic(std::move(topic)) {}

    const std::string& name() const override {
        return m_name;
    }

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_num_batches;
    }

    diaspora::Ordering ordering() const override {
        return m_ordering;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    diaspora::Future<std::optional<diaspora::EventID>> push(
            diaspora::Metadata metadata,
            diaspora::DataView data,
            std::optional<size_t> partition) override;

    diaspora::Future<std::optional<diaspora::Flushed>> flush() override;
};

}

#endif
