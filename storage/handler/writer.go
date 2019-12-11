package handler

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/logger"
	"github.com/lindb/lindb/replication"
	"github.com/lindb/lindb/rpc"
	"github.com/lindb/lindb/rpc/proto/field"
	"github.com/lindb/lindb/rpc/proto/storage"
	"github.com/lindb/lindb/service"
	"github.com/lindb/lindb/tsdb"
)

// Writer implements the stream write service.
type Writer struct {
	storageService service.StorageService
	logger         *logger.Logger
}

// NewWriter returns a new Writer.
func NewWriter(storageService service.StorageService) *Writer {
	return &Writer{
		storageService: storageService,
		logger:         logger.GetLogger("storage", "Writer"),
	}
}

func (w *Writer) Reset(ctx context.Context, req *storage.ResetSeqRequest) (*storage.ResetSeqResponse, error) {
	logicNode, err := getLogicNodeFromCtx(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	sequence, err := w.getSequence(req.Database, req.ShardID, *logicNode)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	sequence.SetHeadSeq(req.Seq)

	return &storage.ResetSeqResponse{}, nil
}

func (w *Writer) Next(ctx context.Context, req *storage.NextSeqRequest) (*storage.NextSeqResponse, error) {
	logicNode, err := getLogicNodeFromCtx(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	sequence, err := w.getSequence(req.Database, req.ShardID, *logicNode)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &storage.NextSeqResponse{Seq: sequence.GetHeadSeq()}, nil
}

// Write handles the stream write request.
func (w *Writer) Write(stream storage.WriteService_WriteServer) error {
	database, shardID, logicNode, err := parseCtx(stream.Context())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	shard, ok := w.storageService.GetShard(database, shardID)
	if !ok {
		return status.Errorf(codes.NotFound, "shard %d for database %s not exists", shardID, database)
	}

	sequence, err := shard.GetOrCreateSequence(logicNode.Indicator())
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			w.logger.Error("error", logger.Error(err))
			return status.Error(codes.Internal, err.Error())
		}

		if len(req.Replicas) == 0 {
			continue
		}

		// nextSeq means the sequence replica wanted
		for _, replica := range req.Replicas {
			seq := replica.Seq

			hs := sequence.GetHeadSeq()
			if hs != seq {
				// reset to headSeq
				return status.Errorf(codes.OutOfRange, "seq num not match replica:%d, storage:%d", seq, hs)
			}

			w.handleReplica(shard, replica)

			sequence.SetHeadSeq(hs + 1)
		}

		resp := &storage.WriteResponse{
			CurSeq: sequence.GetHeadSeq() - 1,
		}

		// add acked seq if synced
		if sequence.Synced() {
			resp.Ack = &storage.WriteResponse_AckSeq{AckSeq: sequence.GetAckSeq()}
		}

		if err := stream.Send(resp); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
}

func (w *Writer) handleReplica(shard tsdb.Shard, replica *storage.Replica) {
	reader := snappy.NewReader(bytes.NewReader(replica.Data))
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		w.logger.Error("decompress replica data error", logger.Error(err))
		return
	}
	var metricList field.MetricList
	err = metricList.Unmarshal(data)
	if err != nil {
		w.logger.Error("unmarshal metricList", logger.Error(err))
		return
	}

	//TODO write metric, need handle panic
	for _, metric := range metricList.Metrics {
		err = shard.Write(metric)
	}
	if err != nil {
		w.logger.Error("write metric", logger.Error(err))
		return
	}
}

func getLogicNodeFromCtx(ctx context.Context) (*models.Node, error) {
	return rpc.GetLogicNodeFromContext(ctx)
}

func parseCtx(ctx context.Context) (database string, shardID int32, logicNode *models.Node, err error) {
	logicNode, err = rpc.GetLogicNodeFromContext(ctx)
	if err != nil {
		return
	}

	database, err = rpc.GetDatabaseFromContext(ctx)
	if err != nil {
		return
	}

	shardID, err = rpc.GetShardIDFromContext(ctx)
	return
}

func (w *Writer) getSequence(database string, shardID int32, logicNode models.Node) (replication.Sequence, error) {
	db, ok := w.storageService.GetDatabase(database)
	if !ok {
		return nil, constants.ErrDatabaseNotFound
	}
	shard, ok := db.GetShard(shardID)
	if !ok {
		return nil, constants.ErrShardNotFound
	}
	return shard.GetOrCreateSequence(logicNode.Indicator())
}
