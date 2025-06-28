cmake_minimum_required(VERSION 3.6.2)
project(ZCache)

SET(CMAKE_CXX_COMPILER "/usr/bin/g++")
# SET(CMAKE_BUILD_TYPE Release)
# set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_C_FLAGS "-O2 -g -Wall -lpthread -lzbd -fsa")
set(CMAKE_CXX_FLAGS "-O2 -g -Wall -lpthread -lzbd")

add_compile_options(-std=c++17)
find_package(PythonLibs REQUIRED)

file(GLOB COMMON_FILES ${PROJECT_SOURCE_DIR}/src/common/*.cpp)
file(GLOB UTILS_FILES ${PROJECT_SOURCE_DIR}/src/utils/*.cpp)
file(GLOB RAW_FILES ${PROJECT_SOURCE_DIR}/src/raw/*.cpp)
file(GLOB KANGAROO_FILES ${PROJECT_SOURCE_DIR}/src/kangaroo/*.cpp)
file(GLOB TIEREDSA_FILES ${PROJECT_SOURCE_DIR}/src/tiered_sa/*.cpp)
file(GLOB SIMU_FILES ${PROJECT_SOURCE_DIR}/src/simu/*.cpp)

file(GLOB MOCK_FILES ${PROJECT_SOURCE_DIR}/src/mock/*.cpp)
file(GLOB MAIN_FILE ${PROJECT_SOURCE_DIR}/src/*.cpp)

include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("/usr/include/python3.10")

foreach(MAIN_FILE ${MAIN_FILE})
    get_filename_component(MAIN_NAME ${MAIN_FILE} NAME_WE)
    add_executable(
        ${MAIN_NAME} ${MAIN_FILE}
        ${COMMON_FILES}
        ${KANGAROO_FILES}
        ${TIEREDSA_FILES}
        ${RAW_FILES}
        ${UTILS_FILES}
        ${SIMU_FILES}
    )
    target_link_libraries(${MAIN_NAME} pthread zbd jsoncpp ${PYTHON_LIBRARIES})
endforeach(MAIN_FILE ${MAIN_FILE})


foreach(MOCK_FILE ${MOCK_FILES})
    get_filename_component(MOCK_NAME ${MOCK_FILE} NAME_WE)
    add_executable(
        ${MOCK_NAME} ${MOCK_FILE}
        ${COMMON_FILES}
    )
    target_link_libraries(${MOCK_NAME} zbd ${PYTHON_LIBRARIES}) 
endforeach(MOCK_FILE ${MOCK_FILES})