package buffer

import (
	"context"

	"github.com/lindb/common/pkg/encoding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	protoCommandV1 "github.com/lindb/lindb/proto/gen/v1/command"
	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/execution/model"
	"github.com/lindb/lindb/sql/planner/plan"
)

type PartitionOutputBuffer struct {
	fragment *plan.PlanFragment
	taskID   model.TaskID
}

func NewPartitionOutputBuffer(taskID model.TaskID, fragment *plan.PlanFragment) OutputBuffer {
	return &PartitionOutputBuffer{
		taskID:   taskID,
		fragment: fragment,
	}
}

// AddPage implements OutputBuffer
func (output *PartitionOutputBuffer) AddPage(page *types.Page) {
	output.sendResultSet(&model.TaskResultSet{
		TaskID: output.taskID,
		NodeID: *output.fragment.RemoteParentNodeID,
		Page:   page,
		NoMore: true, // FIXME: set nomore
	})
}

func (output *PartitionOutputBuffer) SetError(err error) {
	output.sendResultSet(&model.TaskResultSet{
		TaskID: output.taskID,
		NodeID: *output.fragment.RemoteParentNodeID,
		NoMore: true, // FIXME: set nomore
		Error:  err.Error(),
	})
}

func (output *PartitionOutputBuffer) sendResultSet(rs *model.TaskResultSet) {
	// TODO: conn pool?
	receiver := output.fragment.Receivers[0]
	conn, err := grpc.Dial(receiver.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := protoCommandV1.NewResultSetServiceClient(conn)
	// TODO: handle resp/error?
	_, err = client.ResultSet(context.TODO(), &protoCommandV1.ResultSetRequest{
		Payload: encoding.JSONMarshal(rs),
	})
	if err != nil {
		panic(err)
	}
}
