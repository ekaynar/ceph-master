# Try to find cpp_redis
# Once done, this will define
#
# CPP_REDIS_FOUND - system has libcpp_redis
# CPP_REDIS_INCLUDE_DIR - the libcpp_redis include directories
# CPP_REDIS_LIBRARIES - link these to use libcpp_redis

# include dir

find_path(HIREDIS_VIP_INCLUDE_DIR hiredis-vip NO_DEFAULT_PATH PATHS
  /usr/local/lib
  /usr/local/include
  /usr/local/include/hiredis-vip
)


# finally the library itself
set(HIREDIS_VIP_LIBRARY_PATH /usr/local/lib)
find_library(LIBHIREDIS_VIP NAMES hiredis_vip ${HIREDIS_VIP_LIBRARY_PATH})
set(HIREDIS_VIP_LIBRARIES ${LIBHIREDIS_VIP})

# handle the QUIETLY and REQUIRED arguments and set HIREDIS_VIP_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(hiredis_vip DEFAULT_MSG HIREDIS_VIP_LIBRARIES HIREDIS_VIP_INCLUDE_DIR)

mark_as_advanced(HIREDIS_VIP_LIBRARIES HIREDIS_VIP_INCLUDE_DIR)

