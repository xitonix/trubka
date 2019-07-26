EXECUTABLE=trubka
WINDOWS=./bin/windows_amd64
LINUX=./bin/linux_amd64
DARWIN=./bin/darwin_amd64
VERSION=$(shell git describe --tags --abbrev=0)

windows:
	env GOOS=windows GOARCH=amd64 go build -i -v -o $(WINDOWS)/$(EXECUTABLE).exe -ldflags="-s -w -X main.version=$(VERSION)"  *.go

linux:
	env GOOS=linux GOARCH=amd64 go build -i -v -o $(LINUX)/$(EXECUTABLE) -ldflags="-s -w -X main.version=$(VERSION)"  *.go

darwin:
	env GOOS=darwin GOARCH=amd64 go build -i -v -o $(DARWIN)/$(EXECUTABLE) -ldflags="-s -w -X main.version=$(VERSION)"  *.go

build: windows linux darwin ## Build binaries
	@echo version: $(VERSION)

package:
	rm -f ./bin/binaries.zip
	zip -v -r ./bin/binaries.zip ./bin/*

all: build package clean

help: ## Display available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean: ## Remove previous build
	rm -rf $(WINDOWS) $(LINUX) $(DARWIN)

.PHONY: all clean
