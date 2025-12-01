#include "pfs/Consumer.hpp"
#include "pfs/Driver.hpp"
#include "pfs/TopicHandle.hpp"
#include "pfs/ThreadPool.hpp"

#include <condition_variable>

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
    // TODO
    throw diaspora::Exception{"PfsConsumer::pull not implemented"};
}

}
