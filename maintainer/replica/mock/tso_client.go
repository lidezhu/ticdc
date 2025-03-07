// Code generated by MockGen. DO NOT EDIT.
// Source: maintainer/replica/tso_client.go

// Package mock_replica is a generated GoMock package.
package mock_replica

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockTSOClient is a mock of TSOClient interface.
type MockTSOClient struct {
	ctrl     *gomock.Controller
	recorder *MockTSOClientMockRecorder
}

// MockTSOClientMockRecorder is the mock recorder for MockTSOClient.
type MockTSOClientMockRecorder struct {
	mock *MockTSOClient
}

// NewMockTSOClient creates a new mock instance.
func NewMockTSOClient(ctrl *gomock.Controller) *MockTSOClient {
	mock := &MockTSOClient{ctrl: ctrl}
	mock.recorder = &MockTSOClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTSOClient) EXPECT() *MockTSOClientMockRecorder {
	return m.recorder
}

// GetTS mocks base method.
func (m *MockTSOClient) GetTS(ctx context.Context) (int64, int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTS", ctx)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(int64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetTS indicates an expected call of GetTS.
func (mr *MockTSOClientMockRecorder) GetTS(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTS", reflect.TypeOf((*MockTSOClient)(nil).GetTS), ctx)
}
