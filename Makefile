.PHONY: test integration vet build

test:
	go test -count=1 ./...

integration:
	@test -n "$$TEST_DATABASE_URL" || (echo 'TEST_DATABASE_URL is required'; exit 1)
	go test -race -count=1 -timeout=120s ./...
	go vet ./...

vet:
	go vet ./...

build:
	go build -o bin/worker ./cmd/worker
