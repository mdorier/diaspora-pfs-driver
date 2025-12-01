#ifndef DIASPORA_PFS_DRIVER_DRIVER_HPP
#define DIASPORA_PFS_DRIVER_DRIVER_HPP

#include <diaspora/Driver.hpp>
#include <pfs/ThreadPool.hpp>
#include <pfs/TopicHandle.hpp>
#include <pfs/Config.hpp>
#include <shared_mutex>
#include <filesystem>

namespace pfs {

class PfsDriver : public diaspora::DriverInterface,
                  public std::enable_shared_from_this<PfsDriver> {

    PfsConfig m_config;
    std::shared_ptr<diaspora::ThreadPoolInterface> m_default_thread_pool =
        std::make_shared<PfsThreadPool>(diaspora::ThreadCount{0});

    public:

    explicit PfsDriver(PfsConfig config);

    const PfsConfig& config() const { return m_config; }

    void createTopic(std::string_view name,
                     const diaspora::Metadata& options,
                     std::shared_ptr<diaspora::ValidatorInterface> validator,
                     std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                     std::shared_ptr<diaspora::SerializerInterface> serializer) override;

    std::shared_ptr<diaspora::TopicHandleInterface> openTopic(std::string_view name) const override;

    bool topicExists(std::string_view name) const override;

    std::shared_ptr<diaspora::ThreadPoolInterface> defaultThreadPool() const override {
        return m_default_thread_pool;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> makeThreadPool(diaspora::ThreadCount count) const override {
        return std::make_shared<PfsThreadPool>(count);
    }

    static inline std::shared_ptr<diaspora::DriverInterface> create(const diaspora::Metadata& metadata) {
        auto config = PfsConfig::fromMetadata(metadata);
        return std::make_shared<PfsDriver>(config);
    }

private:
    std::shared_ptr<PfsTopicHandle> loadTopic(const std::string& topic_name) const;
    size_t parseNumPartitions(const diaspora::Metadata& options);
    std::string formatPartitionDir(size_t partition_index);
    void saveComponentMetadata(const std::filesystem::path& topic_path,
                                const std::shared_ptr<diaspora::ValidatorInterface>& validator,
                                const std::shared_ptr<diaspora::PartitionSelectorInterface>& selector,
                                const std::shared_ptr<diaspora::SerializerInterface>& serializer);
};

}

#endif
