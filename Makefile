# CMake and build variables
CMAKE_BINARY_DIR = build
CMAKE_SOURCE_DIR = .
# Define the sentinel file (a file that signifies the CMake configuration step is done)
SENTINEL_FILE = $(CMAKE_BINARY_DIR)/.cmake_configured

# Default target
all: build

# Generate CMake configuration - The target is now the SENTINEL_FILE
$(SENTINEL_FILE): $(CMAKE_SOURCE_DIR)/CMakeLists.txt
	@echo "Generating CMake configuration..."
	@mkdir -p $(CMAKE_BINARY_DIR)
	@cmake -S $(CMAKE_SOURCE_DIR) -B $(CMAKE_BINARY_DIR) -DBUILD_TESTING=ON -DBUILD_BENCHMARKS=ON
	@touch $(SENTINEL_FILE)

# Build the project
build: $(SENTINEL_FILE)
	cmake --build build --config Relase -j8

# Clean build directory
clean:
	rm -rf $(CMAKE_BINARY_DIR)

# Rebuild after changes
rebld: clean build

# Run unit tests
unittest: build
	$(CMAKE_BINARY_DIR)/test/unittest/unittest

# Add the sentinel file to the list of PHONY targets if you might want to call it directly
.PHONY: build clean rebld all unittest

