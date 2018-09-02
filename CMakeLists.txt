cmake_minimum_required(VERSION 2.8)
PROJECT(RPC)

set(CMAKE_BUILD_TYPE "Debug")

# We want access to the PRI* print format macros.
add_definitions(-D__STDC_FORMAT_MACROS)

# We want short macros from util/status.h.
add_definitions(-DKUDU_HEADERS_USE_SHORT_STATUS_MACROS=1)

# Slice includes many gutil dependencies that third-party users of the Kudu
# client library don't have. Our build has them, though.
add_definitions(-DKUDU_HEADERS_USE_RICH_SLICE=1)

# We don't want to use any stubs; that's exclusively for builds using our
# exported client headers).
add_definitions(-DKUDU_HEADERS_NO_STUBS=1)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake_modules)
find_package(LibEv REQUIRED)
if (NOT LIBEV_INCLUDE_DIR)
  message(FATAL_ERROR "Fail to find libev lib")
endif()

find_package(GLog REQUIRED)
if (NOT GLOG_INCLUDE_DIR)
  message(FATAL_ERROR "Fail to find glog lib")
endif()

find_package(GFlags REQUIRED)
if (NOT GFLAGS_INCLUDE_DIR)
  message(FATAL_ERROR "Fail to find gflags lib")
endif()

find_package(Boost REQUIRED)
include_directories(${BOOST_INCLUDE_DIRS})

find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIRS})
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${LIBEV_INCLUDE_DIR}/libev)
include_directories(${GLOG_INCLUDE_DIR})
include_directories(${GFLAGS_INCLUDE_DIR})

link_directories(${CMAKE_SOURCE_DIR}/third_party/pink/lib/)
link_directories(${CMAKE_SOURCE_DIR}/third_party/slash/lib/)

find_library(PROTOC_LIB NAMES protoc)
if(NOT PROTOC_LIB)
    message(FATAL_ERROR "Fail to find protoc lib")
endif()

#for c++11
macro(use_cxx11)
if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()
endmacro(use_cxx11)

use_cxx11()

#for proto
file(GLOB PROTOS "${CMAKE_SOURCE_DIR}/proto/*.proto")
list(APPEND PROTO_FLAGS -I${CMAKE_CURRENT_BINARY_DIR})
foreach(PROTO ${PROTOS})
    get_filename_component(PROTO_WE ${PROTO} NAME_WE)
    list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_WE}.pb.cc")
    execute_process(
        COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTO_FLAGS} --cpp_out=${CMAKE_CURRENT_BINARY_DIR} --proto_path=${PROTOBUF_INCLUDE_DIR} --proto_path=${CMAKE_SOURCE_DIR}/proto ${PROTO}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
endforeach()

set(STATIC_LIB
   pink
   slash
)

set(DYNAMIC_LIB
    ${PROTOBUF_LIBRARIES}
    ${PROTOC_LIB}
    ${GLOG_SHARED_LIB}
    ${GFLAGS_SHARED_LIB}
    pthread
    rt
    ssl
    crypto
    dl
    z
)

function(ADD_EXPORTABLE_LIBRARY LIB_NAME)
  # Parse the arguments.
  set(options EXPORTED_SHARED)
  set(one_value_args COMPILE_FLAGS EXPORTED_OUTPUT_NAME EXPORTED_OUTPUT_DIRECTORY)
  set(multi_value_args SRCS DEPS EXPORTED_DEPS NONLINK_DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  # First add the regular version of the library. It uses
  # whatever linkage was defined globally.
  add_library(${LIB_NAME} ${ARG_SRCS})
  if(ARG_COMPILE_FLAGS)
    set_target_properties(${LIB_NAME}
      PROPERTIES COMPILE_FLAGS ${ARG_COMPILE_FLAGS})
  endif()
  target_link_libraries(${LIB_NAME} ${ARG_DEPS})
  if(ARG_NONLINK_DEPS)
    add_dependencies(${LIB_NAME} ${ARG_NONLINK_DEPS})
  endif()
endfunction()

#sub_directory
ADD_SUBDIRECTORY(kudu/gutil)
ADD_SUBDIRECTORY(kudu/util)
ADD_SUBDIRECTORY(kudu/rpc)
ADD_SUBDIRECTORY(example)
