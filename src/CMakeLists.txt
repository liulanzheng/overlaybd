find_package(PkgConfig)
pkg_check_modules(GLIB REQUIRED gio-unix-2.0)

find_library(LIBNL_LIB nl-3)
find_library(LIBNL_GENL_LIB nl-genl-3)
set(LIBNL_LIBS
  ${LIBNL_LIB}
  ${LIBNL_GENL_LIB}
)

find_path(LIBNL_INCLUDE_DIR
  NAMES
  netlink/netlink.h
  PATH_SUFFIXES
  libnl3
)

add_subdirectory(overlaybd)

add_library(tcmu_lib
    STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/strlcpy.c
    ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/configfs.c
    ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/libtcmu.c
    ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/libtcmu_log.c
    ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/libtcmu_config.c
    ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/libtcmu_time.c
    ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/scsi.c
)
target_include_directories(tcmu_lib
  PUBLIC ${LIBNL_INCLUDE_DIR}
  PUBLIC ${GLIB_INCLUDE_DIRS}
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/libtcmu/
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(tcmu_lib
  ${LIBNL_LIBS}
  ${GLIB_LIBRARIES}
  photon_lib
)

set(CURL_STATIC ON)
find_package(CURL REQUIRED)
set(OpenSSL_STATIC ON)
find_package(OpenSSL REQUIRED)

file(GLOB SOURCE_IMAGE
  image_file.cpp
  image_service.cpp
  sure_file.cpp
  switch_file.cpp
  bk_download.cpp
  prefetch.cpp
  prefetch_v2.cpp
  p2p_adaptor.cpp
  common_func.cpp
)

add_library(image_lib STATIC
    ${SOURCE_IMAGE}
)

target_include_directories(image_lib PUBLIC
    ${CURL_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/rapidjson/include
)

add_executable(overlaybd-tcmu main.cpp scsi_helper.cpp)
target_include_directories(overlaybd-tcmu
  PUBLIC libtcmu}
)

target_link_libraries(overlaybd-tcmu
    -Wl,--whole-archive
    base_lib
    image_lib
    fs_lib
    photon_lib
    net_lib
    rpc_lib
    tcmu_lib
    ${CURL_LIBRARIES}
    ${OPENSSL_SSL_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    -Wl,--no-whole-archive

    -lrt
    -lresolv
    -laio
    -lpthread
    -static-libgcc
    -static-libstdc++
)


install(TARGETS overlaybd-tcmu DESTINATION /opt/overlaybd/bin)

if (TARGET_TAG STREQUAL "aliyun")
  install(FILES example_config/overlaybd.aliyun.json DESTINATION /etc/overlaybd/ RENAME overlaybd.json)
elseif (TARGET_TAG STREQUAL "asicloud")
  install(FILES example_config/overlaybd.asicloud.json DESTINATION /etc/overlaybd/ RENAME overlaybd.json)
elseif (TARGET_TAG STREQUAL "fc_rund")
  install(FILES example_config/overlaybd.fc_rund.json DESTINATION /etc/overlaybd/ RENAME overlaybd.json)
else ()
  install(FILES example_config/overlaybd.json DESTINATION /etc/overlaybd/)
endif()

install(FILES example_config/cred.json DESTINATION /opt/overlaybd/)
install(FILES example_config/overlaybd-tcmu.service DESTINATION /usr/lib/systemd/system/)

add_subdirectory(tools)
