.ONESHELL:

format:
	goimports -l -w .

test:
	PARQUETGODEBUG='tracebuf=1' go test ./...
