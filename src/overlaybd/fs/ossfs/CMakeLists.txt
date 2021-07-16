file(GLOB SOURCE_OSSFS
    "*.cpp"
    "./aliyun-oss-cpp-sdk/sdk/src/*.c*"
    "./aliyun-oss-cpp-sdk/sdk/src/*/*.c*"
    "./aliyun-oss-cpp-sdk/sdk/src/external/*/*.c*"
)

add_library(ossfs_lib STATIC ${SOURCE_OSSFS})

target_include_directories(ossfs_lib
  PUBLIC "./aliyun-oss-cpp-sdk/sdk/include"
  PUBLIC "./aliyun-oss-cpp-sdk/sdk/include/alibabacloud/oss"
  PUBLIC "./aliyun-oss-cpp-sdk/sdk/src/"
  PUBLIC "./aliyun-oss-cpp-sdk/sdk/src/external"
  )

target_compile_options(ossfs_lib PUBLIC)

