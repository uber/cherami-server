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
)

const (
	// OperationCreate indicates Create
	OperationCreate Operation = "Create"
	// OperationRead indicates Read
	OperationRead Operation = "Read"
	// OperationUpdate indicates Update
	OperationUpdate Operation = "Update"
	// OperationDelete indicates Delete
	OperationDelete Operation = "Delete"
)

type (
	// Operation is type for an user operation, e.g. CreateDestination, PublishDestination
	Operation string

	// Resource is type for a Cherami resource, e.g. destination, consumer group
	Resource string

	// AuthManager is interface to do auth
	AuthManager interface {
		Authorize(ctx context.Context, operation Operation, resource Resource) error
	}

	// BypassAuthManager is a dummy implementation
	BypassAuthManager struct {
	}
)

// NewBypassAuthManager creates a dummy instance
func NewBypassAuthManager() AuthManager {
	return &BypassAuthManager{}
}

// Authorize authorizes user
func (a *BypassAuthManager) Authorize(ectx context.Context, operation Operation, resource Resource) error {
	return nil
}
