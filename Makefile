
# Makefile for Go builds

# Target platforms for compilation
PLATFORMS := linux/amd64 darwin/arm64 darwin/amd64 windows/amd64
DIST_DIR := dist

# Get current timestamp as version number
VERSION := $(shell date +%Y%m%d%H%M%S)

# Get project name (current directory name)
PROJECT_NAME := $(shell basename $(PWD))

# Detect project structure and get all applications
APPS := $(shell \
	if [ -d cmd ] && [ $$(find cmd -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l) -gt 0 ]; then \
		ls cmd 2>/dev/null; \
	elif [ -d cmd ] && [ -f cmd/main.go ]; then \
		echo $(PROJECT_NAME); \
	elif [ -f main.go ]; then \
		echo $(PROJECT_NAME); \
	fi \
)

# Default target
.PHONY: all clean build release tidy help

all: build

# Build for current platform (development mode)
build: clean tidy
	@mkdir -p $(DIST_DIR)
	@echo "Building for current platform..."
	@$(foreach app,$(APPS), \
		$(call build_dev_app,$(app)) \
	)
	@echo "Build completed!"

# Release build (cross-compilation)
release: clean tidy
	@mkdir -p $(DIST_DIR)
	@echo "Starting release build..."
	@$(foreach platform,$(PLATFORMS), \
		$(call build_release_platform,$(platform)) \
	)
	@echo "Release build completed!"

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	@go mod tidy

# Clean dist directory
clean:
	@rm -rf $(DIST_DIR)
	@echo "Cleanup completed"

# Build single application for development (current platform)
define build_dev_app
	$(eval OUTPUT_NAME=$(DIST_DIR)/$(1))
	@echo "  Building: $(1) -> $(OUTPUT_NAME) (version: $(VERSION))"
	@$(call build_go_dev,$(1),$(OUTPUT_NAME))
endef

# Go development build command
define build_go_dev
	$(if $(shell if [ -d cmd ] && [ $$(find cmd -mindepth 1 -maxdepth 1 -type d | wc -l) -gt 0 ]; then echo "multi"; fi), \
		go build -o $(2) -ldflags "-X main.Version=$(VERSION)" cmd/$(1)/*, \
		$(if $(shell [ -d cmd ] && [ -f cmd/main.go ] && echo "single"), \
			go build -o $(2) -ldflags "-X main.Version=$(VERSION)" cmd/*, \
			go build -o $(2) -ldflags "-X main.Version=$(VERSION)" . \
		) \
	)
endef

# Build all applications for single platform (release mode)
define build_release_platform
	$(eval GOOS=$(word 1,$(subst /, ,$(1))))
	$(eval GOARCH=$(word 2,$(subst /, ,$(1))))
	@echo "Compiling for platform: $(GOOS)/$(GOARCH)"
	@$(foreach app,$(APPS), \
		$(call build_release_app,$(app),$(GOOS),$(GOARCH)) \
	)
endef

# Build single application (release mode)
define build_release_app
	$(eval OUTPUT_NAME=$(DIST_DIR)/$(1)-$(2)-$(3)$(if $(filter windows,$(2)),.exe,))
	@echo "  Building: $(1) -> $(OUTPUT_NAME) (version: $(VERSION))"
	@$(call build_go_release,$(1),$(OUTPUT_NAME),$(2),$(3))
endef

# Go release build command
define build_go_release
	$(if $(shell if [ -d cmd ] && [ $$(find cmd -mindepth 1 -maxdepth 1 -type d | wc -l) -gt 0 ]; then echo "multi"; fi), \
		CGO_ENABLED=0 GOOS=$(3) GOARCH=$(4) go build \
			-o $(2) \
			-trimpath \
			-a \
			-ldflags "-s -w -extldflags -static -X main.Version=$(VERSION)" \
			cmd/$(1)/*, \
		$(if $(shell [ -d cmd ] && [ -f cmd/main.go ] && echo "single"), \
			CGO_ENABLED=0 GOOS=$(3) GOARCH=$(4) go build \
				-o $(2) \
				-trimpath \
				-a \
				-ldflags "-s -w -extldflags -static -X main.Version=$(VERSION)" \
				cmd/*, \
			CGO_ENABLED=0 GOOS=$(3) GOARCH=$(4) go build \
				-o $(2) \
				-trimpath \
				-a \
				-ldflags "-s -w -extldflags -static -X main.Version=$(VERSION)" \
				. \
		) \
	)
endef

# Help information
help:
	@echo "Available targets:"
	@echo "  all     - Build binaries for current platform (default)"
	@echo "  build   - Build binaries for current platform"
	@echo "  release - Cross-compile release versions for all platforms"
	@echo "  tidy    - Tidy Go module dependencies"
	@echo "  clean   - Clean dist directory"
	@echo "  help    - Show this help information"
	@echo ""
	@echo "Project information:"
	@echo "  Project name: $(PROJECT_NAME)"
	@echo "  Detected apps: $(APPS)"
	@echo ""
	@echo "Supported platforms for release:"
	@$(foreach platform,$(PLATFORMS), \
		echo "  $(platform)" ; \
	)