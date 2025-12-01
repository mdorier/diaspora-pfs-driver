#include "pfs/TopicHandle.hpp"
#include "pfs/Driver.hpp"
#include "pfs/Producer.hpp"
#include "pfs/Consumer.hpp"

namespace pfs {

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
