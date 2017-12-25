cmake_minimum_required(VERSION 2.6)
project(gidworker)
file(GLOB GIDWORKER_SOURCE *.c)

find_package(PkgConfig)
pkg_check_modules(GEARMAN REQUIRED gearmand)
link_directories(${GEARMAN_LIBRARY_DIRS})

add_executable(${PROJECT_NAME} ${GIDWORKER_SOURCE})

target_link_libraries(${PROJECT_NAME} ${GEARMAN_LIBRARIES})
target_include_directories(${PROJECT_NAME} PUBLIC ${GEARMAN_INCLUDE_DIRS})
target_compile_options(${PROJECT_NAME} PUBLIC ${GEARMAN_CFLAGS_OTHER})
