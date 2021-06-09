file(GLOB SOURCE_CHECKEDFS "*.cpp" "tool/*.cpp")

add_library(checkedfs_lib STATIC ${SOURCE_CHECKEDFS})

target_compile_options(checkedfs_lib PUBLIC -msse4.2 -mcrc32)

