#include "pfs/TopicHandle.hpp"
#include "pfs/Driver.hpp"
#include "pfs/Producer.hpp"
#include "pfs/Consumer.hpp"

namespace pfs {

PfsTopicHandle::PfsTopicHandle(
    std::string name,
    std::string topic_path,
    size_t num_partitions,
    std::vector<diaspora::PartitionInfo> pinfo,
    diaspora::Validator validator,
    diaspora::PartitionSelector partition_selector,
    diaspora::Serializer serializer,
    PfsConfig config,
    std::shared_ptr<PfsDriver> driver)
: m_name{std::move(name)}
, m_topic_path{std::move(topic_path)}
, m_pinfo{std::move(pinfo)}
, m_validator(std::move(validator))
, m_partition_selector(std::move(partition_selector))
, m_serializer(std::move(serializer))
, m_config(std::move(config))
, m_driver{std::move(driver)}
{
    // Initialize partition file handles
    m_partitions.reserve(num_partitions);
    for (size_t i = 0; i < num_partitions; ++i) {
        std::string partition_path = m_topic_path + "/partitions/" + formatPartitionDir(i);
        m_partitions.push_back(
            std::make_unique<PartitionFiles>(
                partition_path,
                m_config.use_file_locking,
                m_config.flush_behavior
            )
        );
    }
}

std::shared_ptr<diaspora::DriverInterface> PfsTopicHandle::driver() const {
    return m_driver;
}

std::shared_ptr<diaspora::ProducerInterface>
PfsTopicHandle::makeProducer(std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        diaspora::Ordering ordering,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        diaspora::Metadata options) {
    (void)options;
    if(!thread_pool) thread_pool = m_driver->makeThreadPool(diaspora::ThreadCount{0});
    auto simple_thread_pool = std::dynamic_pointer_cast<PfsThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw diaspora::Exception{"ThreadPool should be an instance of PfsThreadPool"};
    return std::make_shared<PfsProducer>(
            std::string{name}, batch_size, max_batch, ordering, simple_thread_pool,
            shared_from_this());
}

std::shared_ptr<diaspora::ConsumerInterface>
PfsTopicHandle::makeConsumer(std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector,
        const std::vector<size_t>& targets,
        diaspora::Metadata options) {
    (void)options;
    (void)targets;
    if(!thread_pool) thread_pool = m_driver->makeThreadPool(diaspora::ThreadCount{0});
    auto simple_thread_pool = std::dynamic_pointer_cast<PfsThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw diaspora::Exception{"ThreadPool should be an instance of PfsThreadPool"};
    return std::make_shared<PfsConsumer>(
            std::string{name}, batch_size, max_batch, simple_thread_pool,
            shared_from_this(), std::move(data_allocator),
            std::move(data_selector));
}

}
