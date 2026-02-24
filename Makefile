.PHONY: generate gen-bazel clean-bazel clean

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

gen-bazel:
	go run github.com/bazelbuild/bazel-gazelle/cmd/gazelle@v0.37.0 update \
		--go_prefix=github.com/cockroachdb/basaltclient --repo_root=. \
		--go_naming_convention=import --go_naming_convention_external=import \
		--proto=disable

clean-bazel:
	git clean -dxf WORKSPACE BUILD.bazel '**/BUILD.bazel'

clean:
	rm -f basaltpb/*.pb.go
