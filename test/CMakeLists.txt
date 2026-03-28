add_library(espjsondb_test_harness INTERFACE)

target_include_directories(
	espjsondb_test_harness
	INTERFACE
	${CMAKE_CURRENT_LIST_DIR}/..
	${CMAKE_CURRENT_LIST_DIR}/../src
)
