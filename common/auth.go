// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"context"
	"errors"
)

type (
	// Operation is type for an user operation, e.g. CreateDestination, PublishDestination
	Operation string

	// Resource is type for a Cherami resource, e.g. Destination, ConsumerGroup
	Resource struct {
		Type string
		Name string
	}

	// Authorizer is interface to do authorization
	Authorizer interface {
		Authorize(ctx context.Context, operation Operation, resource Resource) (bool, error)
	}

	// BypassAuthorizerImpl is a dummy implementation for Authorizer
	BypassAuthorizerImpl struct {
	}
)

// EmptyResource is an empty resource which is used when authorization does not need resource
var EmptyResource = Resource{
	Type: "",
	Name: "",
}

// ErrorUnauthorized is an error for unahorized
var ErrorUnauthorized = errors.New("Unauthorized")

// NewBypassAuthorizer creates a BypassAuthorizerImpl instance
func NewBypassAuthorizer() Authorizer {
	return &BypassAuthorizerImpl{}
}

// Authorize authorizes user
func (a *BypassAuthorizerImpl) Authorize(ctx context.Context, operation Operation, resource Resource) (bool, error) {
	return true, nil
}
