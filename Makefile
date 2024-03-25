start:
	go run main.go
proto gen:
	cd ./proto &&  protoc --go_out=. --go-grpc_out=. *.proto && cd ../
mock gen:
	mockery --dir ./internal/service --all --output ./internal/rpc/mocks --with-expecter
