#include "pfs/ComponentSerializer.hpp"
#include <diaspora/Exception.hpp>
#include <fstream>

namespace pfs {

void ComponentSerializer::saveValidator(const std::filesystem::path& path,
                                          const diaspora::Validator& validator) {
    if (!validator) {
        // If no validator, save empty object
        std::ofstream ofs(path);
        if (!ofs) {
            throw diaspora::Exception{
                "Failed to open file for writing: " + path.string()
            };
        }
        ofs << "{}";
        return;
    }

    try {
        auto metadata = validator.metadata();
        std::ofstream ofs(path);
        if (!ofs) {
            throw diaspora::Exception{
                "Failed to open file for writing: " + path.string()
            };
        }
        ofs << metadata.dump(2);  // Pretty print with 2-space indent
    } catch (const std::exception& e) {
        throw diaspora::Exception{
            "Failed to save validator: " + std::string(e.what())
        };
    }
}

void ComponentSerializer::saveSerializer(const std::filesystem::path& path,
                                           const diaspora::Serializer& serializer) {
    if (!serializer) {
        // If no serializer, save empty object
        std::ofstream ofs(path);
        if (!ofs) {
            throw diaspora::Exception{
                "Failed to open file for writing: " + path.string()
            };
        }
        ofs << "{}";
        return;
    }

    try {
        auto metadata = serializer.metadata();
        std::ofstream ofs(path);
        if (!ofs) {
            throw diaspora::Exception{
                "Failed to open file for writing: " + path.string()
            };
        }
        ofs << metadata.dump(2);
    } catch (const std::exception& e) {
        throw diaspora::Exception{
            "Failed to save serializer: " + std::string(e.what())
        };
    }
}

void ComponentSerializer::savePartitionSelector(const std::filesystem::path& path,
                                                  const diaspora::PartitionSelector& selector) {
    if (!selector) {
        // If no selector, save empty object
        std::ofstream ofs(path);
        if (!ofs) {
            throw diaspora::Exception{
                "Failed to open file for writing: " + path.string()
            };
        }
        ofs << "{}";
        return;
    }

    try {
        auto metadata = selector.metadata();
        std::ofstream ofs(path);
        if (!ofs) {
            throw diaspora::Exception{
                "Failed to open file for writing: " + path.string()
            };
        }
        ofs << metadata.dump(2);
    } catch (const std::exception& e) {
        throw diaspora::Exception{
            "Failed to save partition selector: " + std::string(e.what())
        };
    }
}

diaspora::Validator ComponentSerializer::loadValidator(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        throw diaspora::Exception{
            "Validator file does not exist: " + path.string()
        };
    }

    try {
        std::ifstream ifs(path);
        if (!ifs) {
            throw diaspora::Exception{
                "Failed to open validator file: " + path.string()
            };
        }

        std::string content((std::istreambuf_iterator<char>(ifs)),
                           std::istreambuf_iterator<char>());

        diaspora::Metadata metadata(content);

        // Check if it's an empty object
        if (metadata.json().empty() ||
            (metadata.json().is_object() && metadata.json().size() == 0)) {
            return diaspora::Validator();  // Return null validator
        }

        return diaspora::Validator::FromMetadata(metadata);
    } catch (const diaspora::Exception&) {
        throw;  // Re-throw diaspora exceptions
    } catch (const std::exception& e) {
        throw diaspora::Exception{
            "Failed to load validator: " + std::string(e.what())
        };
    }
}

diaspora::Serializer ComponentSerializer::loadSerializer(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        throw diaspora::Exception{
            "Serializer file does not exist: " + path.string()
        };
    }

    try {
        std::ifstream ifs(path);
        if (!ifs) {
            throw diaspora::Exception{
                "Failed to open serializer file: " + path.string()
            };
        }

        std::string content((std::istreambuf_iterator<char>(ifs)),
                           std::istreambuf_iterator<char>());

        diaspora::Metadata metadata(content);

        // Check if it's an empty object
        if (metadata.json().empty() ||
            (metadata.json().is_object() && metadata.json().size() == 0)) {
            return diaspora::Serializer();  // Return null serializer
        }

        return diaspora::Serializer::FromMetadata(metadata);
    } catch (const diaspora::Exception&) {
        throw;  // Re-throw diaspora exceptions
    } catch (const std::exception& e) {
        throw diaspora::Exception{
            "Failed to load serializer: " + std::string(e.what())
        };
    }
}

diaspora::PartitionSelector ComponentSerializer::loadPartitionSelector(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        throw diaspora::Exception{
            "Partition selector file does not exist: " + path.string()
        };
    }

    try {
        std::ifstream ifs(path);
        if (!ifs) {
            throw diaspora::Exception{
                "Failed to open partition selector file: " + path.string()
            };
        }

        std::string content((std::istreambuf_iterator<char>(ifs)),
                           std::istreambuf_iterator<char>());

        diaspora::Metadata metadata(content);

        // Check if it's an empty object
        if (metadata.json().empty() ||
            (metadata.json().is_object() && metadata.json().size() == 0)) {
            return diaspora::PartitionSelector();  // Return null selector
        }

        return diaspora::PartitionSelector::FromMetadata(metadata);
    } catch (const diaspora::Exception&) {
        throw;  // Re-throw diaspora exceptions
    } catch (const std::exception& e) {
        throw diaspora::Exception{
            "Failed to load partition selector: " + std::string(e.what())
        };
    }
}

}
