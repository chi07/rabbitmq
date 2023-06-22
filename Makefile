GO_BINARY_NAME=apperror

export

install:
	npm update -g
	npm i -g husky
	go install github.com/joho/godotenv/cmd/godotenv@latest
	go mod tidy

ci.lint:
	@echo "== 🙆 ci.linter =="
	golangci-lint run -v ./... --fix

test-report:
	go test ./... -coverprofile coverage.out
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html

coverage:
	go tool cover -html=coverage.out