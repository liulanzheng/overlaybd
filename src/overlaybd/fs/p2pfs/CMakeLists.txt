file(GLOB SOURCE_P2PFS "*.cpp" "tool/*.cpp")

add_library(p2pfs_lib STATIC ${SOURCE_P2PFS})

target_compile_options(p2pfs_lib PUBLIC)

