#ifndef DIASPORA_PFS_DRIVER_COMPONENT_SERIALIZER_HPP
#define DIASPORA_PFS_DRIVER_COMPONENT_SERIALIZER_HPP

#include <diaspora/Validator.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/PartitionSelector.hpp>
#include <diaspora/Metadata.hpp>
#include <filesystem>
#include <memory>

namespace pfs {

/**
 * @brief Utility class for serializing and deserializing Diaspora components
 * (Validator, Serializer, PartitionSelector) to/from JSON files.
 *
 * Uses the Diaspora API's built-in metadata() and FromMetadata() methods.
 */
class ComponentSerializer {
public:
    /**
     * @brief Save a Validator to a JSON file
     * @param path Path to the JSON file
     * @param validator Validator to serialize
     */
    static void saveValidator(const std::filesystem::path& path,
                               const diaspora::Validator& validator);

    /**
     * @brief Save a Serializer to a JSON file
     * @param path Path to the JSON file
     * @param serializer Serializer to serialize
     */
    static void saveSerializer(const std::filesystem::path& path,
                                const diaspora::Serializer& serializer);

    /**
     * @brief Save a PartitionSelector to a JSON file
     * @param path Path to the JSON file
     * @param selector PartitionSelector to serialize
     */
    static void savePartitionSelector(const std::filesystem::path& path,
                                       const diaspora::PartitionSelector& selector);

    /**
     * @brief Load a Validator from a JSON file
     * @param path Path to the JSON file
     * @return Validator instance
     */
    static diaspora::Validator loadValidator(const std::filesystem::path& path);

    /**
     * @brief Load a Serializer from a JSON file
     * @param path Path to the JSON file
     * @return Serializer instance
     */
    static diaspora::Serializer loadSerializer(const std::filesystem::path& path);

    /**
     * @brief Load a PartitionSelector from a JSON file
     * @param path Path to the JSON file
     * @return PartitionSelector instance
     */
    static diaspora::PartitionSelector loadPartitionSelector(const std::filesystem::path& path);
};

}

#endif
