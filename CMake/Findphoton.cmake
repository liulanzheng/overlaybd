include(FetchContent)

FetchContent_Declare(
  photon
  GIT_REPOSITORY https://github.com/alibaba/PhotonLibOS.git
  GIT_TAG cebbab2d045da77d416bbc82d1b1e7ea13edce42
)
FetchContent_GetProperties(photon)
if (NOT photon_POPULATED)
  FetchContent_Populate(photon)
endif()

set(PHOTON_INCLUDE_DIR ${photon_SOURCE_DIR}/include/)
