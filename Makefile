.PHONY: generate clean

# Generate Go code from proto files
generate:
	protoc \
		--go_out=. --go_opt=module=github.com/cockroachdb/basaltclient \
		--go-grpc_out=. --go-grpc_opt=module=github.com/cockroachdb/basaltclient \
		-I. \
		basaltpb/*.proto

clean:
	rm -f basaltpb/*.pb.go
