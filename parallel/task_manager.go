package parallel

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/rpc"
	pb "github.com/lindb/lindb/rpc/proto/common"
)

//go:generate mockgen -source=./task_manager.go -destination=./task_manager_mock.go -package=parallel

// TaskManager represents the task manager for current node
type TaskManager interface {
	// AllocTaskID allocates the task id for new task, before task submits
	AllocTaskID() string
	// Submit submits the task, saving task context for task tracking
	Submit(taskCtx TaskContext)
	// Complete completes the task by task id
	Complete(taskID string)
	// Get returns the task context by task id
	Get(taskID string) TaskContext

	// SendRequest sends the task request to target node based on node's indicator
	SendRequest(targetNodeID string, req *pb.TaskRequest) error
	// SendResponse sends the task response to parent node
	SendResponse(targetNodeID string, resp *pb.TaskResponse) error
}

// taskManager implements the task manager interface, tracks all task of the current node
type taskManager struct {
	currentNodeID     string
	seq               int64
	taskClientFactory rpc.TaskClientFactory
	taskServerFactory rpc.TaskServerFactory

	tasks sync.Map
}

// NewTaskManager creates the task manager
func NewTaskManager(currentNode models.Node,
	taskClientFactory rpc.TaskClientFactory, taskServerFactory rpc.TaskServerFactory) TaskManager {
	return &taskManager{
		currentNodeID:     (&currentNode).Indicator(),
		taskClientFactory: taskClientFactory,
		taskServerFactory: taskServerFactory,
	}
}

// AllocTaskID allocates the task id for new task, before task submits
func (t *taskManager) AllocTaskID() string {
	seq := atomic.AddInt64(&t.seq, 1)
	return fmt.Sprintf("%s-%d", t.currentNodeID, seq)
}

// Submit submits the task, saving task context for task tracking
func (t *taskManager) Submit(taskCtx TaskContext) {
	//TODO check duplicate
	t.tasks.Store(taskCtx.TaskID(), taskCtx)
}

// Complete completes the task by task id
func (t *taskManager) Complete(taskID string) {
	t.tasks.Delete(taskID)
}

// Get returns the task context by task id
func (t *taskManager) Get(taskID string) TaskContext {
	task, ok := t.tasks.Load(taskID)
	if !ok {
		return nil
	}
	taskCtx, ok := task.(TaskContext)
	if !ok {
		return nil
	}
	return taskCtx
}

// SendRequest sends the task request to target node based on node's indicator,
// if fail, returns err
func (t *taskManager) SendRequest(targetNodeID string, req *pb.TaskRequest) error {
	client := t.taskClientFactory.GetTaskClient(targetNodeID)
	if client == nil {
		return errNoSendStream
	}
	if err := client.Send(req); err != nil {
		return errTaskSend
	}
	return nil
}

// SendResponse sends the task response to parent node,
// if fail, returns err
func (t *taskManager) SendResponse(parentNodeID string, resp *pb.TaskResponse) error {
	stream := t.taskServerFactory.GetStream(parentNodeID)
	if stream == nil {
		return errNoSendStream
	}
	if err := stream.Send(resp); err != nil {
		return err
	}
	return nil
}
