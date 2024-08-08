cmake_minimum_required(VERSION 2.6)
project(zdw)

add_definitions(-D_FILE_OFFSET_BITS=64 -D__STDC_LIMIT_MACROS -Wall -Wold-style-cast)

add_library(zdw
	BufferedInput.cpp
	BufferedOutput.cpp
	ConvertToZDW.cpp
	ConvertToZDW.h
	UnconvertFromZDW.cpp
	dictionary.cpp
	dictionary.h
	getnextrow.cpp
	getnextrow.h
	memory.cpp
	memory.h
	status_output.cpp
	stringheap.cpp
	stringheap.h
	zdw_column_type_constants.h
	zdw/BufferedInput.h
	zdw/BufferedOutput.h
	zdw/UnconvertFromZDW.h
	zdw/includes.h
	zdw/status_output.h
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

set_property(TARGET zdw PROPERTY CXX_STANDARD 11)
set_property(TARGET unconvertDWfile PROPERTY CXX_STANDARD 11)
set_property(TARGET convertDWfile PROPERTY CXX_STANDARD 11)
