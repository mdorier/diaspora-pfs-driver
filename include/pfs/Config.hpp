#ifndef DIASPORA_PFS_DRIVER_CONFIG_HPP
#define DIASPORA_PFS_DRIVER_CONFIG_HPP

#include <diaspora/Metadata.hpp>
#include <string>

namespace pfs {

struct PfsConfig {
    enum class FlushBehavior {
        IMMEDIATE,    // fsync() after every write
        ON_FLUSH,     // fsync() only when flush() is called
        BUFFERED      // Let OS handle buffering (no fsync)
    };

    std::string root_path;
    bool use_file_locking;
    FlushBehavior flush_behavior;

    // Default configuration
    PfsConfig()
    : root_path("./diaspora_data")
    , use_file_locking(false)
    , flush_behavior(FlushBehavior::BUFFERED)
    {}

    PfsConfig(std::string path, bool file_locking, FlushBehavior flush)
    : root_path(std::move(path))
    , use_file_locking(file_locking)
    , flush_behavior(flush)
    {}

    // Parse configuration from Diaspora metadata JSON string
    static PfsConfig fromMetadata(const diaspora::Metadata& metadata);
};

}

#endif
