# set source files
file (GLOB src-files "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (diaspora-pfs-driver-pkg "share/cmake/diaspora-pfs-driver")

# set version fo shared library
set (diaspora-pfs-vers "${DIASPORA_PFS_DRIVER_VERSION_MAJOR}.${DIASPORA_PFS_DRIVER_VERSION_MINOR}")

# target library
add_library (diaspora_pfs ${src-files})
target_link_libraries (diaspora_pfs
    PUBLIC diaspora-stream-api
    PRIVATE coverage_config warnings_config)
target_include_directories (diaspora_pfs PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (diaspora_pfs BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (diaspora_pfs BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (diaspora_pfs
    PROPERTIES VERSION ${DIASPORA_PFS_DRIVER_VERSION}
    SOVERSION ${DIASPORA_PFS_DRIVER_VERSION_MAJOR})

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "diaspora-pfs-driver-config-version.cmake"
    VERSION ${DIASPORA_PFS_DRIVER_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (diaspora-pfs-driver-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/diaspora-pfs-driver-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/mofka/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

install (TARGETS diaspora_pfs
         EXPORT diaspora-pfs-driver-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT diaspora-pfs-driver-targets
         DESTINATION ${diaspora-pfs-driver-pkg}
         FILE "diaspora-pfs-driver-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/diaspora-pfs-driver-config.cmake"
         "${CMAKE_CURRENT_BINARY_DIR}/diaspora-pfs-driver-config-version.cmake"
         DESTINATION ${diaspora-pfs-driver-pkg} )
install (DIRECTORY ../include/pfs
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
