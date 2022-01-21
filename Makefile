proto:
	protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf --gofast_out=plugins=grpc,paths=source_relative:.  proto/cnode.proto
