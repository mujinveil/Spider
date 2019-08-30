cmake_minimum_required( VERSION 2.8 )
project( vo1 )

set( CMAKE_BUILD_TYPE "Release" )
set( CMAKE_CXX_FLAGS "-std=c++11 -O3" )


find_package( OpenCV REQUIRED ) # use this if in OpenCV2

include_directories(
    ${OpenCV_INCLUDE_DIRS}
    ${CSPARSE_INCLUDE_DIR}
    "/usr/include/eigen3/"
)


add_executable(imgmatch imgmatch.cpp )
target_link_libraries(imgmatch
   ${OpenCV_LIBS}
   ${CSPARSE_LIBRARY}

)
