#include "pfs/Driver.hpp"
#include "pfs/ComponentSerializer.hpp"
#include <nlohmann/json.hpp>
#include <fstream>
#include <sstream>
#include <iomanip>

namespace pfs {

DIASPORA_REGISTER_DRIVER(_, diaspora_pfs, PfsDriver);

PfsDriver::PfsDriver(PfsConfig config)
: m_config(std::move(config))
{
    // Create root directory if it doesn't exist
    try {
        std::filesystem::create_directories(m_config.root_path);
    } catch (const std::filesystem::filesystem_error& e) {
        throw diaspora::Exception{
            "Failed to create root directory: " + std::string(e.what())
        };
    }

    // Load existing topics
    loadExistingTopics();
}

void PfsDriver::loadExistingTopics() {
    namespace fs = std::filesystem;

    if (!fs::exists(m_config.root_path)) {
        return;  // No topics yet
    }

    for (const auto& entry : fs::directory_iterator(m_config.root_path)) {
        if (!entry.is_directory()) continue;

        std::string topic_name = entry.path().filename().string();

        try {
            auto topic = loadTopic(topic_name, entry.path());
            m_topics[topic_name] = topic;
        } catch (const diaspora::Exception& e) {
            // Log warning but continue loading other topics
            // For now, just skip invalid topics
        }
    }
}

std::shared_ptr<PfsTopicHandle> PfsDriver::loadTopic(
    const std::string& topic_name,
    const std::filesystem::path& topic_path) {

    namespace fs = std::filesystem;

    try {
        // Load component metadata from JSON files
        auto validator = ComponentSerializer::loadValidator(topic_path / "validator.json");
        auto serializer = ComponentSerializer::loadSerializer(topic_path / "serializer.json");
        auto selector = ComponentSerializer::loadPartitionSelector(topic_path / "partition-selector.json");

        // Discover partitions by scanning the partitions directory
        auto partitions_path = topic_path / "partitions";
        if (!fs::exists(partitions_path) || !fs::is_directory(partitions_path)) {
            throw diaspora::Exception{
                "Partitions directory not found for topic: " + topic_name
            };
        }

        // Count and validate partitions
        size_t max_partition = 0;
        bool found_any = false;

        for (const auto& entry : fs::directory_iterator(partitions_path)) {
            if (!entry.is_directory()) continue;

            std::string dirname = entry.path().filename().string();

            // Parse partition number from "00000000", "00000001", etc.
            try {
                size_t partition_num = std::stoull(dirname);
                max_partition = std::max(max_partition, partition_num);
                found_any = true;
            } catch (...) {
                // Skip non-numeric directories
                continue;
            }
        }

        if (!found_any) {
            throw diaspora::Exception{
                "No valid partitions found for topic: " + topic_name
            };
        }

        size_t num_partitions = max_partition + 1;

        // Build partition info vector
        std::vector<diaspora::PartitionInfo> pinfo;
        for (size_t i = 0; i < num_partitions; ++i) {
            pinfo.push_back(diaspora::PartitionInfo{"{}"});
        }

        // Set partitions in selector
        if (selector) {
            selector.setPartitions(pinfo);
        }

        // Create topic handle
        return std::make_shared<PfsTopicHandle>(
            topic_name,
            topic_path.string(),
            num_partitions,
            pinfo,
            std::move(validator),
            std::move(selector),
            std::move(serializer),
            m_config,
            shared_from_this()
        );

    } catch (const diaspora::Exception&) {
        throw;  // Re-throw diaspora exceptions
    } catch (const std::exception& e) {
        throw diaspora::Exception{
            "Failed to load topic '" + topic_name + "': " + std::string(e.what())
        };
    }
}

size_t PfsDriver::parseNumPartitions(const diaspora::Metadata& options) {
    try {
        const auto& json = options.json();
        if (json.contains("num_partitions")) {
            return json["num_partitions"].get<size_t>();
        }
    } catch (...) {
        // If parsing fails, return default
    }

    return 1;  // Default to 1 partition
}

std::string PfsDriver::formatPartitionDir(size_t partition_index) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(8) << partition_index;
    return oss.str();
}

void PfsDriver::saveComponentMetadata(
    const std::filesystem::path& topic_path,
    const std::shared_ptr<diaspora::ValidatorInterface>& validator,
    const std::shared_ptr<diaspora::PartitionSelectorInterface>& selector,
    const std::shared_ptr<diaspora::SerializerInterface>& serializer) {

    try {
        // Save validator
        ComponentSerializer::saveValidator(
            topic_path / "validator.json",
            diaspora::Validator(validator)
        );

        // Save serializer
        ComponentSerializer::saveSerializer(
            topic_path / "serializer.json",
            diaspora::Serializer(serializer)
        );

        // Save partition selector
        ComponentSerializer::savePartitionSelector(
            topic_path / "partition-selector.json",
            diaspora::PartitionSelector(selector)
        );
    } catch (const diaspora::Exception& e) {
        throw diaspora::Exception{
            "Failed to save component metadata: " + std::string(e.what())
        };
    }
}

void PfsDriver::createTopic(std::string_view name,
                             const diaspora::Metadata& options,
                             std::shared_ptr<diaspora::ValidatorInterface> validator,
                             std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                             std::shared_ptr<diaspora::SerializerInterface> serializer) {
    std::unique_lock lock(m_topics_mutex);

    if(m_topics.count(std::string{name})) {
        throw diaspora::Exception{"Topic already exists"};
    }

    // Parse num_partitions from options
    size_t num_partitions = parseNumPartitions(options);

    // Create directory structure
    namespace fs = std::filesystem;
    fs::path topic_path = fs::path(m_config.root_path) / std::string{name};

    try {
        fs::create_directories(topic_path);
        fs::create_directories(topic_path / "partitions");

        // Create partition directories
        std::vector<diaspora::PartitionInfo> pinfo;
        for (size_t i = 0; i < num_partitions; ++i) {
            std::string partition_dir = formatPartitionDir(i);
            fs::create_directories(topic_path / "partitions" / partition_dir);
            pinfo.push_back(diaspora::PartitionInfo{"{}"});
        }

        // Set partitions in selector
        if(selector) selector->setPartitions(pinfo);

        // Save component metadata to JSON files
        saveComponentMetadata(topic_path, validator, selector, serializer);

        // Create topic handle
        m_topics.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(std::string{name}),
            std::forward_as_tuple(
                std::make_shared<PfsTopicHandle>(
                    std::string{name},
                    topic_path.string(),
                    num_partitions,
                    pinfo,
                    std::move(validator),
                    std::move(selector),
                    std::move(serializer),
                    m_config,
                    shared_from_this()
                )
            )
        );

    } catch (const fs::filesystem_error& e) {
        throw diaspora::Exception{
            "Failed to create topic directory structure: " + std::string(e.what())
        };
    }
}

}
