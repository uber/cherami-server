package common

import "github.com/uber/tchannel-go/thrift"
import (
	athrift "github.com/apache/thrift/lib/go/thrift"
)

type (
	TChanAuthorizerFilter struct {
		ChanServer thrift.TChanServer
		Authorizer Authorizer
	}
)

func (f *TChanAuthorizerFilter) Handle(ctx thrift.Context, methodName string, protocol athrift.TProtocol) (success bool, resp athrift.TStruct, err error) {
	// Here we do a very high level validation to verify user has access on the operation.
	// We use an empty resource because there is no generic way to get resource from each request.
	// Each service could do a business logic specific authorization to further verify the user.
	authorized, err := f.Authorizer.Authorize(ctx, Operation(methodName), EmptyResource)
	if err != nil {
		return false, nil, err
	}

	if !authorized {
		return false, nil, UnauthorizedError
	}

	return f.ChanServer.Handle(ctx, methodName, protocol)
}

func (f *TChanAuthorizerFilter) Service() string {
	return f.ChanServer.Service()
}

func (f *TChanAuthorizerFilter) Methods() []string {
	return f.ChanServer.Methods()
}
