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

package metadata

import "github.com/uber/cherami-thrift/.generated/go/shared"
import "github.com/uber/cherami-thrift/.generated/go/metadata"
import "github.com/stretchr/testify/mock"

import "github.com/uber/tchannel-go/thrift"

// TChanMetadataExposable is an autogenerated mock type for the TChanMetadataExposable type
type TChanMetadataExposable struct {
	mock.Mock
}

// HostAddrToUUID provides a mock function with given fields: ctx, hostAddr
func (_m *TChanMetadataExposable) HostAddrToUUID(ctx thrift.Context, hostAddr string) (string, error) {
	ret := _m.Called(ctx, hostAddr)

	var r0 string
	if rf, ok := ret.Get(0).(func(thrift.Context, string) string); ok {
		r0 = rf(ctx, hostAddr)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, string) error); ok {
		r1 = rf(ctx, hostAddr)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListExtentsStats provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ListExtentsStats(ctx thrift.Context, request *shared.ListExtentsStatsRequest) (*shared.ListExtentsStatsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *shared.ListExtentsStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *shared.ListExtentsStatsRequest) *shared.ListExtentsStatsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.ListExtentsStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *shared.ListExtentsStatsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListHosts provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ListHosts(ctx thrift.Context, request *metadata.ListHostsRequest) (*metadata.ListHostsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ListHostsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ListHostsRequest) *metadata.ListHostsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ListHostsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ListHostsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListInputHostExtentsStats provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ListInputHostExtentsStats(ctx thrift.Context, request *metadata.ListInputHostExtentsStatsRequest) (*metadata.ListInputHostExtentsStatsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ListInputHostExtentsStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ListInputHostExtentsStatsRequest) *metadata.ListInputHostExtentsStatsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ListInputHostExtentsStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ListInputHostExtentsStatsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListStoreExtentsStats provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ListStoreExtentsStats(ctx thrift.Context, request *metadata.ListStoreExtentsStatsRequest) (*metadata.ListStoreExtentsStatsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ListStoreExtentsStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ListStoreExtentsStatsRequest) *metadata.ListStoreExtentsStatsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ListStoreExtentsStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ListStoreExtentsStatsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroupExtent provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadConsumerGroupExtent(ctx thrift.Context, request *metadata.ReadConsumerGroupExtentRequest) (*metadata.ReadConsumerGroupExtentResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ReadConsumerGroupExtentResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ReadConsumerGroupExtentRequest) *metadata.ReadConsumerGroupExtentResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReadConsumerGroupExtentResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ReadConsumerGroupExtentRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroupExtents provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadConsumerGroupExtents(ctx thrift.Context, request *metadata.ReadConsumerGroupExtentsRequest) (*metadata.ReadConsumerGroupExtentsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ReadConsumerGroupExtentsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ReadConsumerGroupExtentsRequest) *metadata.ReadConsumerGroupExtentsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReadConsumerGroupExtentsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ReadConsumerGroupExtentsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadExtentStats provides a mock function with given fields: ctx, request
func (_m *TChanMetadataExposable) ReadExtentStats(ctx thrift.Context, request *metadata.ReadExtentStatsRequest) (*metadata.ReadExtentStatsResult_, error) {
	ret := _m.Called(ctx, request)

	var r0 *metadata.ReadExtentStatsResult_
	if rf, ok := ret.Get(0).(func(thrift.Context, *metadata.ReadExtentStatsRequest) *metadata.ReadExtentStatsResult_); ok {
		r0 = rf(ctx, request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*metadata.ReadExtentStatsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *metadata.ReadExtentStatsRequest) error); ok {
		r1 = rf(ctx, request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UUIDToHostAddr provides a mock function with given fields: ctx, hostUUID
func (_m *TChanMetadataExposable) UUIDToHostAddr(ctx thrift.Context, hostUUID string) (string, error) {
	ret := _m.Called(ctx, hostUUID)

	var r0 string
	if rf, ok := ret.Get(0).(func(thrift.Context, string) string); ok {
		r0 = rf(ctx, hostUUID)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, string) error); ok {
		r1 = rf(ctx, hostUUID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
