.PHONY: test lint run web

test:
	go test -race -cover ./...

lint:
	go vet ./...

run:
	go run ./cmd/relayforge

web:
	cd web && npm install && npm run dev
