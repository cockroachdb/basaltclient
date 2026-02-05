.PHONY: generate clean

GOGOPROTO := $(shell go list -m -f '{{.Dir}}' github.com/gogo/protobuf)

# Generate Go code from proto files using gogoproto
generate:
	protoc \
		--gogofast_out=plugins=grpc,\
Mgogoproto/gogo.proto=github.com/gogo/protobuf/gogoproto,\
Mbasaltpb/common.proto=github.com/cockroachdb/basaltclient/basaltpb,\
Mbasaltpb/controller.proto=github.com/cockroachdb/basaltclient/basaltpb,\
Mbasaltpb/blob.proto=github.com/cockroachdb/basaltclient/basaltpb,\
paths=source_relative:. \
		-I. -I$(GOGOPROTO) \
		basaltpb/*.proto

clean:
	rm -f basaltpb/*.pb.go
