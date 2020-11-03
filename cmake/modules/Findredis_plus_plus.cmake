
# <------------ add redis-plus-plus dependency -------------->
# NOTE: this should be *sw* NOT *redis++*
find_path(REDIS_PLUS_PLUS_INCLUDE_DIR sw NO_DEFAULT_PATH PATHS
  /usr/local/lib
  /usr/local/include
  /usr/local/include/sw
)

#target_include_directories(target PUBLIC ${REDIS_PLUS_PLUS_HEADER})

set(REDIS_PLUS_PLUS_LIB_PATH /usr/local/lib)
find_library(LIBREDIS_PLUS_PLUS NAMES redis++ ${REDIS_PLUS_PLUS_LIB_PATH})
set(REDIS_PLUS_PLUS_LIB ${LIBREDIS_PLUS_PLUS})
#target_link_libraries(target ${REDIS_PLUS_PLUS_LIB})

# handle the QUIETLY and REQUIRED arguments and set CPP_REDIS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(redis++ DEFAULT_MSG REDIS_PLUS_PLUS_LIB REDIS_PLUS_PLUS_INCLUDE_DIR)

mark_as_advanced(REDIS_PLUS_PLUS_LIB REDIS_PLUS_PLUS_INCLUDE_DIR)


