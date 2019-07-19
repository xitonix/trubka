EXECUTABLE=trubka
WINDOWS=./bin/windows_amd64/$(EXECUTABLE).exe
LINUX=./bin/linux_amd64/$(EXECUTABLE)
DARWIN=./bin/darwin_amd64/$(EXECUTABLE)
VERSION=$(shell git describe --tags --always --long)

windows:
	env GOOS=windows GOARCH=amd64 go build -i -v -o $(WINDOWS) -ldflags="-s -w -X main.version=$(VERSION)"  *.go

linux:
	env GOOS=linux GOARCH=amd64 go build -i -v -o $(LINUX) -ldflags="-s -w -X main.version=$(VERSION)"  *.go

darwin:
	env GOOS=darwin GOARCH=amd64 go build -i -v -o $(DARWIN) -ldflags="-s -w -X main.version=$(VERSION)"  *.go

build: windows linux darwin ## Build binaries
	@echo version: $(VERSION)

all: build

help: ## Display available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean: ## Remove previous build
	rm -f $(WINDOWS) $(LINUX) $(DARWIN)

.PHONY: all clean
