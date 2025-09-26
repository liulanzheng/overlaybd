file(GLOB SOURCE_LSMT "*.cpp")

add_library(lsmt_lib STATIC ${SOURCE_LSMT})
target_include_directories(lsmt_lib PUBLIC
    ${PHOTON_INCLUDE_DIR}
)

# Check for AVX512F support and add appropriate compiler flags
# include(CheckCXXCompilerFlag)
# check_cxx_compiler_flag("-mavx512f" COMPILER_SUPPORTS_AVX512F)
# if(COMPILER_SUPPORTS_AVX512F)
#     target_compile_options(lsmt_lib PRIVATE "-mavx512f")
# endif()

if(BUILD_TESTING)
  add_subdirectory(test)
endif()
