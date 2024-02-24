run: build
	@./bin/db-init

build:
	@go build -o ./bin/db-init
