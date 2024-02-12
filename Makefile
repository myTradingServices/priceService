start:
	go run main.go
gen:
	cd ./proto &&  protoc --go_out=. --go-grpc_out=. *.proto && cd ../