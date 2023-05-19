.PHONY: test

all: build test basic

prep:
	go mod tidy

clean:
	go clean
	go mod tidy

build: prep
	go build .

test: build
	go test -v ./... -coverprofile="./test-coverage.out" -count=1

test_coverage: test
	go tool cover -html="./test-coverage.out" -o "./test-coverage.html"

test_coverage_html: test_coverage
	open "./test-coverage.html"

basic: build
	go run . basic --repo-working ./repos/working --cycles 1 --repo-finished ./repos/finished
