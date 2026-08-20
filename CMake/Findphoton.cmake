include(FetchContent)
set(FETCHCONTENT_QUIET false)
set(PHOTON_ENABLE_EXTFS ON CACHE BOOL "Build Photon extfs support" FORCE)
set(PHOTON_ENABLE_RESIZE ON CACHE BOOL "Build Photon extfs resize support" FORCE)
set(PHOTON_BUILD_OCF_CACHE ON CACHE BOOL "Build Photon OCF cache support" FORCE)
add_definitions(-DPHOTON_ENABLE_RESIZE)

FetchContent_Declare(
  photon
  GIT_REPOSITORY https://github.com/alibaba/PhotonLibOS.git
  GIT_TAG release/0.9
)

if(BUILD_TESTING)
  set(BUILD_TESTING 0)
  FetchContent_MakeAvailable(photon)
  set(BUILD_TESTING 1)
else()
  FetchContent_MakeAvailable(photon)
endif()

if (BUILD_CURL_FROM_SOURCE)
  find_package(OpenSSL REQUIRED)
  find_package(CURL REQUIRED)
  add_dependencies(photon_obj CURL::libcurl OpenSSL::SSL OpenSSL::Crypto)
endif()

if(NOT ORIGIN_EXT2FS)
  add_dependencies(photon_obj libext2fs)
endif()

set(PHOTON_INCLUDE_DIR ${photon_SOURCE_DIR}/include/)
