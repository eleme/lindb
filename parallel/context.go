package parallel

import (
	"errors"
	"sync/atomic"

	"github.com/lindb/lindb/models"
	pb "github.com/lindb/lindb/rpc/proto/common"
	"github.com/lindb/lindb/series"
	"github.com/lindb/lindb/sql/stmt"
)

type TaskType int

const (
	RootTask TaskType = iota + 1
	IntermediateTask
)

type JobContext interface {
	Plan() *models.PhysicalPlan
	Query() *stmt.Query
	Emit(event *series.TimeSeriesEvent)
	Complete()
	ResultSet() chan *series.TimeSeriesEvent
}

type jobContext struct {
	resultSet chan *series.TimeSeriesEvent
	plan      *models.PhysicalPlan
	query     *stmt.Query
}

func NewJobContext(resultSet chan *series.TimeSeriesEvent, plan *models.PhysicalPlan, query *stmt.Query) JobContext {
	return &jobContext{
		resultSet: resultSet,
		plan:      plan,
		query:     query,
	}
}

func (c *jobContext) Plan() *models.PhysicalPlan {
	return c.plan
}

func (c *jobContext) Query() *stmt.Query {
	return c.query
}
func (c *jobContext) ResultSet() chan *series.TimeSeriesEvent {
	return c.resultSet
}

func (c *jobContext) Complete() {
	//TODO send result
	close(c.resultSet)
}

func (c *jobContext) Emit(event *series.TimeSeriesEvent) {
	c.resultSet <- event
}

// TaskContext represents the task context for distribution query and computing
type TaskContext interface {
	// TaskID returns the task id under current node
	TaskID() string
	// Type returns the task type
	TaskType() TaskType
	// ParentNode returns the parent node's indicator for sending task result
	ParentNode() string
	// ParentTaskID returns the parent node's task id for tracking task
	ParentTaskID() string
	// ReceiveResult marks receive result, decreases the num. of task tracking
	ReceiveResult(resp *pb.TaskResponse)
	// Completed returns if the task is completes
	Completed() bool
	// Error returns task's error
	Error() error
}

// taskContext represents the task context for tacking task execution state
type taskContext struct {
	taskID       string
	taskType     TaskType
	parentTaskID string
	parentNode   string
	merger       ResultMerger

	err           error
	expectResults int32
}

// newTaskContext creates the task context based on params
func newTaskContext(taskID string, taskType TaskType, parentTaskID string, parentNode string,
	expectResults int32, merger ResultMerger) TaskContext {
	return &taskContext{
		taskID:        taskID,
		taskType:      taskType,
		parentTaskID:  parentTaskID,
		parentNode:    parentNode,
		merger:        merger,
		expectResults: expectResults,
	}
}

func (c *taskContext) TaskType() TaskType {
	return c.taskType
}

// ParentNode returns the parent node's indicator for sending task result
func (c *taskContext) ParentNode() string {
	return c.parentNode
}

// ParentTaskID returns the parent node's task id for tracking task
func (c *taskContext) ParentTaskID() string {
	return c.parentTaskID
}

// TaskID returns the task id under current node
func (c *taskContext) TaskID() string {
	return c.taskID
}

// ReceiveResult marks receive result, decreases the num. of task tracking,
// if no pending task marks this task completed
func (c *taskContext) ReceiveResult(resp *pb.TaskResponse) {
	if len(resp.ErrMsg) > 0 {
		atomic.StoreInt32(&c.expectResults, 0)
		c.err = errors.New(resp.ErrMsg)
		return
	}

	//FIXME stone1100 add lock????
	c.merger.Merge(resp)

	// if task is completed, reduces expect result count
	if resp.Completed {
		atomic.AddInt32(&c.expectResults, -1)
	}
}

// Error returns task's error
func (c *taskContext) Error() error {
	return c.err
}

// Completed returns if the task is completes
func (c *taskContext) Completed() bool {
	return atomic.LoadInt32(&c.expectResults) == 0
}
