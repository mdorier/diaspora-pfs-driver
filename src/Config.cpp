#include "pfs/Config.hpp"
#include <diaspora/Exception.hpp>

namespace pfs {

PfsConfig PfsConfig::fromMetadata(const diaspora::Metadata& metadata) {
    PfsConfig config;

    try {
        const auto& json = metadata.json();

        if (json.contains("root_path")) {
            config.root_path = json["root_path"].get<std::string>();
        }

        if (json.contains("use_file_locking")) {
            config.use_file_locking = json["use_file_locking"].get<bool>();
        }

        if (json.contains("flush_behavior")) {
            std::string flush_str = json["flush_behavior"].get<std::string>();
            if (flush_str == "immediate") {
                config.flush_behavior = FlushBehavior::IMMEDIATE;
            } else if (flush_str == "on_flush") {
                config.flush_behavior = FlushBehavior::ON_FLUSH;
            } else if (flush_str == "buffered") {
                config.flush_behavior = FlushBehavior::BUFFERED;
            } else {
                throw diaspora::Exception{
                    "Invalid flush_behavior: " + flush_str +
                    ". Valid values are: immediate, on_flush, buffered"
                };
            }
        }

    } catch (const nlohmann::json::type_error& e) {
        throw diaspora::Exception{
            "Invalid type in PfsConfig metadata: " + std::string(e.what())
        };
    }

    return config;
}

}
