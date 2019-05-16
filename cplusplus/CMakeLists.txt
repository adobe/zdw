cmake_minimum_required(VERSION 2.6)
project(zdw)

add_definitions(-D_FILE_OFFSET_BITS=64 -D__STDC_LIMIT_MACROS -Wall)

add_library(zdw
	UnconvertFromZDW.cpp
	UnconvertFromZDW.h
	memory.cpp
	memory.h
	BufferedInput.h
	BufferedOutput.cpp
	ConvertToZDW.cpp
	ConvertToZDW.h
	dictionary.cpp
	dictionary.h
	getnextrow.cpp
	getnextrow.h
	stringheap.cpp
	stringheap.h
	includes.h
	zdw_column_type_constants.h
)

add_executable(convertDWfile
	convertDWfile.cpp
)

add_executable(unconvertDWfile
	unconvertDWfile.cpp
)

find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

include_directories( zdw ${CMAKE_CURRENT_SOURCE_DIR} ${ZLIB_INCLUDE_DIRS} )

# for cmake 2.6 compatibility, can't automatically handle include files
# target_include_directories( zdw PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${ZLIB_INCLUDE_DIRS} )

target_link_libraries(zdw ${ZLIB_LIBRARIES})
target_link_libraries(unconvertDWfile zdw)
target_link_libraries(convertDWfile zdw)

