include_directories($ENV{GFLAGS}/include)
link_directories($ENV{GFLAGS}/lib)

include_directories($ENV{GTEST}/googletest/include)
link_directories($ENV{GTEST}/lib)

add_executable(lsmt_test test.cpp)
target_include_directories(lsmt_test PUBLIC ${PHOTON_INCLUDE_DIR})
target_link_libraries(lsmt_test gtest gtest_main gflags pthread photon_static overlaybd_lib)

add_test(
  NAME lsmt_test
  COMMAND ${EXECUTABLE_OUTPUT_PATH}/lsmt_test
)

# include(CheckCXXCompilerFlag)
# check_cxx_compiler_flag("-mavx512f" COMPILER_SUPPORTS_AVX512F)
# if (COMPILER_SUPPORTS_AVX512F)
#   add_executable(lsmt_test_lbt test.cpp)
#   target_include_directories(lsmt_test_lbt PUBLIC ${PHOTON_INCLUDE_DIR})
#   target_link_libraries(lsmt_test_lbt gtest gtest_main gflags pthread photon_static overlaybd_lib)
#   target_compile_options(lsmt_test_lbt PRIVATE "-mavx512f")

#   add_test(
#     NAME lsmt_test_lbt
#     COMMAND ${EXECUTABLE_OUTPUT_PATH}/lsmt_test_lbt
#   )
# endif()
