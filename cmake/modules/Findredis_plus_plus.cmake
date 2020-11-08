
# <------------ add redis-plus-plus dependency -------------->
# NOTE: this should be *sw* NOT *redis++*
find_path(REDIS_PLUS_PLUS_HEADER sw)
#NO_DEFAULT_PATH PATHS
#  /usr/local/lib
#  /usr/local/include
#  /usr/local/include/sw/redis++/
#)

#find_path(REDIS_PLUS_PLUS_HEADER sw)
#target_include_directories(rgw PUBLIC ${REDIS_PLUS_PLUS_HEADER})
#target_include_directories(rgw_a PUBLIC ${REDIS_PLUS_PLUS_HEADER})

#find_library(REDIS_PLUS_PLUS_LIB redis++)
#target_link_libraries(rgw ${REDIS_PLUS_PLUS_LIB})
#target_link_libraries(rgw_a ${REDIS_PLUS_PLUS_LIB})


set(REDIS_PLUS_PLUS_LIB_PATH /usr/local/lib)
find_library(LIBREDIS_PLUS_PLUS NAMES redis++ ${REDIS_PLUS_PLUS_LIB_PATH})
set(REDIS_PLUS_PLUS_LIB ${LIBREDIS_PLUS_PLUS})

# handle the QUIETLY and REQUIRED arguments and set CPP_REDIS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(redis++ DEFAULT_MSG REDIS_PLUS_PLUS_LIB REDIS_PLUS_PLUS_HEADER)

mark_as_advanced(REDIS_PLUS_PLUS_LIB REDIS_PLUS_PLUS_HEADER)


