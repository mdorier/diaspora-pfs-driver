#ifndef DIASPORA_PFS_DRIVER_TOPIC_HANDLE_HPP
#define DIASPORA_PFS_DRIVER_TOPIC_HANDLE_HPP

#include <diaspora/TopicHandle.hpp>
#include <pfs/PartitionFiles.hpp>
#include <pfs/Config.hpp>

#include <vector>
#include <memory>
#include <sstream>
#include <iomanip>

namespace pfs {

class PfsDriver;

class PfsTopicHandle final : public diaspora::TopicHandleInterface,
                             public std::enable_shared_from_this<PfsTopicHandle> {

    friend class PfsProducer;
    friend class PfsConsumer;

    const std::string                          m_name;
    const std::string                          m_topic_path;
    const std::vector<diaspora::PartitionInfo> m_pinfo;
    const diaspora::Validator                  m_validator;
    const diaspora::PartitionSelector          m_partition_selector;
    const diaspora::Serializer                 m_serializer;
    const PfsConfig                            m_config;
    const std::shared_ptr<PfsDriver>           m_driver;

    std::vector<std::unique_ptr<PartitionFiles>> m_partitions;

    public:

    PfsTopicHandle(
        std::string name,
        std::string topic_path,
        size_t num_partitions,
        std::vector<diaspora::PartitionInfo> pinfo,
        diaspora::Validator validator,
        diaspora::PartitionSelector partition_selector,
        diaspora::Serializer serializer,
        PfsConfig config,
        std::shared_ptr<PfsDriver> driver);

    PartitionFiles& getPartition(size_t index) {
        if (index >= m_partitions.size()) {
            throw diaspora::Exception{"Invalid partition index"};
        }
        return *m_partitions[index];
    }

    static std::string formatPartitionDir(size_t index) {
        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(8) << index;
        return oss.str();
    }

    const std::string& name() const override {
        return m_name;
    }

    std::shared_ptr<diaspora::DriverInterface> driver() const override;

    const std::vector<diaspora::PartitionInfo>& partitions() const override {
        return m_pinfo;
    }

    diaspora::Validator validator() const override {
        return m_validator;
    }

    diaspora::PartitionSelector selector() const override {
        return m_partition_selector;
    }

    diaspora::Serializer serializer() const override {
        return m_serializer;
    }

    std::shared_ptr<diaspora::ProducerInterface>
        makeProducer(std::string_view name,
                     diaspora::BatchSize batch_size,
                     diaspora::MaxNumBatches max_batch,
                     diaspora::Ordering ordering,
                     std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                     diaspora::Metadata options) override;

    std::shared_ptr<diaspora::ConsumerInterface>
        makeConsumer(std::string_view name,
                     diaspora::BatchSize batch_size,
                     diaspora::MaxNumBatches max_batch,
                     std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                     diaspora::DataAllocator data_allocator,
                     diaspora::DataSelector data_selector,
                     const std::vector<size_t>& targets,
                     diaspora::Metadata options) override;
};

}

#endif
