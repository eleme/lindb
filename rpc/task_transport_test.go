package rpc

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/lindb/lindb/models"
	commonmock "github.com/lindb/lindb/rpc/pbmock/common"
	"github.com/lindb/lindb/rpc/proto/common"
)

const testGRPCPort = 9999

func TestTaskServerFactory(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	fct := NewTaskServerFactory()

	stream := fct.GetStream((&node).Indicator())
	assert.Nil(t, stream)

	mockServerStream := commonmock.NewMockTaskService_HandleServer(ctl)

	epoch := fct.Register((&node).Indicator(), mockServerStream)
	stream = fct.GetStream((&node).Indicator())
	assert.NotNil(t, stream)

	nodes := fct.Nodes()
	assert.Equal(t, 1, len(nodes))
	assert.Equal(t, node, nodes[0])

	ok := fct.Deregister(10, (&node).Indicator())
	assert.False(t, ok)
	ok = fct.Deregister(epoch, (&node).Indicator())
	assert.True(t, ok)
	// parse node error
	fct.Register("node_err", mockServerStream)
	nodes = fct.Nodes()
	assert.Equal(t, 0, len(nodes))
}

func TestTaskClientFactory(t *testing.T) {
	ctl := gomock.NewController(t)
	defer func() {
		ctl.Finish()
	}()

	mockClientConnFct := NewMockClientConnFactory(ctl)

	mockTaskClient := commonmock.NewMockTaskService_HandleClient(ctl)
	mockTaskClient.EXPECT().Recv().Return(nil, nil).AnyTimes()
	mockTaskClient.EXPECT().CloseSend().Return(fmt.Errorf("err")).AnyTimes()
	taskService := commonmock.NewMockTaskServiceClient(ctl)

	fct := NewTaskClientFactory(models.Node{IP: "127.0.0.1", Port: 123})
	receiver := NewMockTaskReceiver(ctl)
	receiver.EXPECT().Receive(gomock.Any()).Return(fmt.Errorf("err")).AnyTimes()
	fct.SetTaskReceiver(receiver)
	fct1 := fct.(*taskClientFactory)
	fct1.connFct = mockClientConnFct
	fct1.newTaskServiceClientFunc = func(cc *grpc.ClientConn) common.TaskServiceClient {
		return taskService
	}

	target := models.Node{IP: "127.0.0.1", Port: testGRPCPort}
	conn, _ := grpc.Dial(target.Indicator(), grpc.WithInsecure())
	mockClientConnFct.EXPECT().GetClientConn(target).Return(conn, nil).AnyTimes()
	taskService.EXPECT().Handle(gomock.Any(), gomock.Any()).Return(mockTaskClient, nil).AnyTimes()
	err := fct.CreateTaskClient(target)
	assert.NoError(t, err)
	tc := fct1.taskStreams[(&target).Indicator()]
	tc.running.Store(false)
	tc.cli = mockTaskClient

	// not create new one if exist
	target = models.Node{IP: "127.0.0.1", Port: testGRPCPort}
	err = fct.CreateTaskClient(target)
	assert.NoError(t, err)

	cli := fct.GetTaskClient((&target).Indicator())
	assert.NotNil(t, cli)

	fct.CloseTaskClient((&target).Indicator())
}

func TestTaskClientFactory_handler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer func() {
		ctrl.Finish()
	}()

	receiver := NewMockTaskReceiver(ctrl)
	fct := NewTaskClientFactory(models.Node{IP: "127.0.0.1", Port: 123})
	fct.SetTaskReceiver(receiver)

	target := models.Node{IP: "127.0.0.1", Port: 321}
	conn, _ := grpc.Dial(target.Indicator(), grpc.WithInsecure())
	mockClientConnFct := NewMockClientConnFactory(ctrl)
	mockTaskClient := commonmock.NewMockTaskService_HandleClient(ctrl)
	mockTaskClient.EXPECT().CloseSend().Return(fmt.Errorf("err")).AnyTimes()
	taskService := commonmock.NewMockTaskServiceClient(ctrl)

	factory := fct.(*taskClientFactory)
	factory.newTaskServiceClientFunc = func(cc *grpc.ClientConn) common.TaskServiceClient {
		return taskService
	}
	factory.connFct = mockClientConnFct
	taskClient := &taskClient{
		targetID: "test",
		target:   target,
	}
	taskClient.running.Store(true)
	gomock.InOrder(
		mockClientConnFct.EXPECT().GetClientConn(target).Return(nil, fmt.Errorf("err")),
		mockClientConnFct.EXPECT().GetClientConn(target).Return(conn, nil),
		taskService.EXPECT().Handle(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("err")),
		mockClientConnFct.EXPECT().GetClientConn(target).Return(conn, nil),
		taskService.EXPECT().Handle(gomock.Any(), gomock.Any()).Return(mockTaskClient, nil),
		mockTaskClient.EXPECT().Recv().Return(nil, fmt.Errorf("err")),
		mockClientConnFct.EXPECT().GetClientConn(target).Return(conn, nil),
		taskService.EXPECT().Handle(gomock.Any(), gomock.Any()).Return(mockTaskClient, nil),
		mockTaskClient.EXPECT().Recv().Return(nil, nil),
		receiver.EXPECT().Receive(gomock.Any()).Return(nil),
		mockTaskClient.EXPECT().Recv().Return(nil, nil),
		receiver.EXPECT().Receive(gomock.Any()).DoAndReturn(func(req *common.TaskResponse) error {
			taskClient.running.Store(false)
			return fmt.Errorf("err")
		}),
	)
	factory.handleTaskResponse(taskClient)
}
