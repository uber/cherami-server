package common

import "github.com/uber/tchannel-go/thrift"
import (
	athrift "github.com/apache/thrift/lib/go/thrift"
)

type (
	// TChanAuthorizerFilter intercepts each tchannal call and applies authorization
	TChanAuthorizerFilter struct {
		ChanServer thrift.TChanServer
		Authorizer Authorizer
	}
)

// Handle implements the method from TChanServer
func (f *TChanAuthorizerFilter) Handle(ctx thrift.Context, methodName string, protocol athrift.TProtocol) (success bool, resp athrift.TStruct, err error) {
	// Here we do a very high level validation to verify user has access on the operation.
	// We use an empty resource because there is no generic way to get resource from each request.
	// Each service could do a business logic specific authorization to further verify the user.
	// TODO we may create a map to change methodName to operation name so the operation name is more user friendly.
	authorized, err := f.Authorizer.Authorize(ctx, Operation(methodName), EmptyResource)
	if err != nil {
		return false, nil, err
	}

	if !authorized {
		return false, nil, ErrorUnauthorized
	}

	return f.ChanServer.Handle(ctx, methodName, protocol)
}

// Service implements the method from TChanServer
func (f *TChanAuthorizerFilter) Service() string {
	return f.ChanServer.Service()
}

// Methods implements the method from TChanServer
func (f *TChanAuthorizerFilter) Methods() []string {
	return f.ChanServer.Methods()
}
