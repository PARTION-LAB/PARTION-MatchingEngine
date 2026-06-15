cmake_minimum_required(VERSION 3.16)

project(partion_matching_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(nlohmann_json QUIET)
find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp)
find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafka.h REQUIRED)
find_library(RDKAFKA_LIBRARY rdkafka REQUIRED)

add_executable(partion-matching-engine
        src/main.cpp
)

target_link_libraries(partion-matching-engine
        PRIVATE
        ${RDKAFKA_LIBRARY}
)

target_include_directories(partion-matching-engine
        PRIVATE
        ${RDKAFKA_INCLUDE_DIR}
)

if (nlohmann_json_FOUND)
    target_link_libraries(partion-matching-engine PRIVATE nlohmann_json::nlohmann_json)
else ()
    target_include_directories(partion-matching-engine PRIVATE ${NLOHMANN_JSON_INCLUDE_DIR})
endif ()
