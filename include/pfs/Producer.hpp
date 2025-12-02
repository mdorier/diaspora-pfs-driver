#ifndef DIASPORA_PFS_DRIVER_PRODUCER_HPP
#define DIASPORA_PFS_DRIVER_PRODUCER_HPP

#include <pfs/ThreadPool.hpp>
#include <pfs/TopicHandle.hpp>
#include <pfs/WriteBatch.hpp>
#include <diaspora/Producer.hpp>
#include <mutex>

namespace pfs {

class PfsProducer final : public diaspora::ProducerInterface {

    const std::string                     m_name;
    const diaspora::BatchSize             m_batch_size;
    const diaspora::MaxNumBatches         m_max_num_batches;
    const diaspora::Ordering              m_ordering;
    const std::shared_ptr<PfsThreadPool>  m_thread_pool;
    const std::shared_ptr<PfsTopicHandle> m_topic;

    // Batch management - one batch per partition
    std::vector<WriteBatch>               m_partition_batches;
    std::vector<std::mutex>               m_batch_mutexes;

    public:

    ~PfsProducer();

    PfsProducer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        diaspora::Ordering ordering,
        std::shared_ptr<PfsThreadPool> thread_pool,
        std::shared_ptr<PfsTopicHandle> topic);

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
