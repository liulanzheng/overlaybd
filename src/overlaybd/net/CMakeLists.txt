file(GLOB SOURCE_NET "*.cpp")

set(CURL_STATIC ON)
find_package(CURL REQUIRED)
set(OpenSSL_STATIC ON)
find_package(OpenSSL REQUIRED)

add_library(net_lib STATIC ${SOURCE_NET})
target_include_directories(net_lib PUBLIC
    ${CURL_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
)
