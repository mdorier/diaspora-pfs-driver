set (DIASPORA_PFS_DRIVER_TEST_BACKEND_ARGS "{}")
set (DIASPORA_PFS_DRIVER_TEST_TOPIC_ARGS "{}")

add_test (NAME DiasporaTestSuite COMMAND
          diaspora-run-tests.sh diaspora_pfs
                                --before ${CMAKE_CURRENT_SOURCE_DIR}/pre-test.sh
                                --after ${CMAKE_CURRENT_SOURCE_DIR}/post-test.sh
                                --backend-args ${DIASPORA_PFS_DRIVER_TEST_BACKEND_ARGS}
                                --topic-args ${DIASPORA_PFS_DRIVER_TEST_TOPIC_ARGS})
set_property (TEST DiasporaTestSuite PROPERTY
              ENVIRONMENT "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}")

add_test (NAME DiasporaPfsDriverBenchmark COMMAND
          ${CMAKE_CURRENT_SOURCE_DIR}/run-benchmark.sh)
set_property (TEST DiasporaPfsDriverBenchmark PROPERTY
              ENVIRONMENT
              "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/src:$ENV{LD_LIBRARY_PATH}"
              "PYTHONPATH=${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH}")
