# set source files
file (GLOB src-files "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (pfs-pkg "share/cmake/pfs")

# set version fo shared library
set (pfs-vers "${diaspora_pfs_driver_VERSION_MAJOR}.${diaspora_pfs_driver_VERSION_MINOR}")

# target library
add_library (diaspora_pfs_driver ${src-files})
target_link_libraries (diaspora_pfs_driver
    PUBLIC diaspora-stream-api
    PRIVATE coverage_config warnings_config)
target_include_directories (diaspora_pfs_driver PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (diaspora_pfs_driver BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (diaspora_pfs_driver BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (diaspora_pfs_driver
    PROPERTIES VERSION ${DIASPORA_PFS_DRIVER_VERSION}
    SOVERSION ${DIASPORA_PFS_DRIVER_VERSION_MAJOR})

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "pfs-config-version.cmake"
    VERSION ${DIASPORA_PFS_DRIVER_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (pfs-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/pfs-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/mofka/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

install (TARGETS diaspora_pfs_driver
         EXPORT pfs-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT pfs-targets
         DESTINATION ${pfs-pkg}
         FILE "pfs-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/pfs-config.cmake"
         "${CMAKE_CURRENT_BINARY_DIR}/pfs-config-version.cmake"
         DESTINATION ${pfs-pkg} )
install (DIRECTORY ../include/pfs
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
