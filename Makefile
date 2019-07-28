.DEFAULT_GOAL := all

EXECUTABLE=trubka
WINDOWS=./bin/windows_amd64
LINUX=./bin/linux_amd64
DARWIN=./bin/darwin_amd64
VERSION=$(shell git describe --tags --abbrev=0)

prepare:
	@echo Cleaning the bin directory
	@rm -rfv ./bin/*

windows:
	@echo Building Windows amd64 binaries
	@env GOOS=windows GOARCH=amd64 go build -i -v -o $(WINDOWS)/$(EXECUTABLE).exe -ldflags="-s -w -X main.version=$(VERSION)"  *.go

linux:
	@echo Building Linux amd64 binaries
	@env GOOS=linux GOARCH=amd64 go build -i -v -o $(LINUX)/$(EXECUTABLE) -ldflags="-s -w -X main.version=$(VERSION)"  *.go

darwin:
	@echo Building Mac amd64 binaries
	@env GOOS=darwin GOARCH=amd64 go build -i -v -o $(DARWIN)/$(EXECUTABLE) -ldflags="-s -w -X main.version=$(VERSION)"  *.go

build: ## Builds the binaries.
build: windows linux darwin
	@echo Version: $(VERSION)

test: ##  Runs the unit tests.
	@echo Running unit tests
	@go test -count=1 ./...

package:
	@echo Creating the zip file
	@tar -C $(DARWIN) -cvzf ./bin/trubka_darwin-$(VERSION).tar.gz $(EXECUTABLE)
	@zip -j ./bin/trubka_windows-$(VERSION).zip $(WINDOWS)/$(EXECUTABLE).exe
	@tar -C $(LINUX) -cvzf ./bin/trubka_linux-$(VERSION).tar.gz $(EXECUTABLE)
	@echo Darwin Checksum:
	@shasum -a 256 ./bin/trubka_darwin-$(VERSION).tar.gz

install:
	@cp -pv $(DARWIN)/$(EXECUTABLE)

help: ##  Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

all: test prepare build package clean

clean: ## Removes the artifacts.
	@rm -rf $(WINDOWS) $(LINUX) $(DARWIN)

.PHONY: all
