

# <------------ add hiredis dependency --------------->
find_path(HIREDIS_INCLUDE_DIR hiredis NO_DEFAULT_PATH PATHS
  /usr/local/lib
  /usr/local/include
  /usr/local/include/hiredis
)

set(HIREDIS_LIBRARY_PATH /usr/local/lib)
find_library(LIBHIREDIS NAMES hiredis ${HIREDIS_LIBRARY_PATH})
set(HIREDIS_LIB ${LIBHIREDIS})



# handle the QUIETLY and REQUIRED arguments and set CPP_REDIS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(hiredis DEFAULT_MSG HIREDIS_LIB HIREDIS_INCLUDE_DIR)

mark_as_advanced(HIREDIS_LIB HIREDIS_INCLUDE_DIR)


