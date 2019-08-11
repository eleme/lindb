package task

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/pkg/state"
)

func TestExecutor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := state.NewMockRepository(ctrl)

	node := models.Node{IP: "1.1.1.1", Port: 8000}
	exec := NewExecutor(context.TODO(), &node, repo)
	proc := &dummyProcessor{}
	exec.Register(proc)
	assert.NotNil(t, exec.processors[proc.Kind()])

	time.AfterFunc(100*time.Millisecond, func() {
		err := exec.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
	repo.EXPECT().WatchPrefix(gomock.Any(), gomock.Any(), true).Return(nil)
	exec.Run()
	exec = NewExecutor(context.TODO(), &node, repo)
	exec.Register(&dummyProcessor{})
	eventCh := make(chan *state.Event)
	time.AfterFunc(100*time.Millisecond, func() {
		close(eventCh)
	})
	repo.EXPECT().WatchPrefix(gomock.Any(), gomock.Any(), true).Return(eventCh)
	exec.Run()

	eventCh = make(chan *state.Event)
	time.AfterFunc(300*time.Millisecond, func() {
		err := exec.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
	repo.EXPECT().WatchPrefix(gomock.Any(), gomock.Any(), true).Return(eventCh)
	go func() {
		sendEvent(eventCh, &state.Event{
			Type: state.EventTypeAll,
		})
		sendEvent(eventCh, &state.Event{
			Type: state.EventTypeDelete,
		})
		sendEvent(eventCh, &state.Event{
			Err: fmt.Errorf("err"),
		})
		task := Task{}
		sendEvent(eventCh, &state.Event{
			Type: state.EventTypeModify,
			KeyValues: []state.EventKeyValue{
				{Key: "xxx", Value: encoding.JSONMarshal(&task)},
			},
		})
	}()
	exec.Run()
}

func TestExecutor_dispatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := state.NewMockRepository(ctrl)
	txn := state.NewMockTransaction(ctrl)
	repo.EXPECT().NewTransaction().Return(txn)
	txn.EXPECT().ModRevisionCmp(gomock.Any(), gomock.Any(), gomock.Any())
	txn.EXPECT().Put(gomock.Any(), gomock.Any())
	repo.EXPECT().Commit(gomock.Any(), txn).Return(nil)

	proc := NewMockProcessor(ctrl)
	proc.EXPECT().Concurrency().Return(0)
	proc.EXPECT().RetryCount().Return(0)
	proc.EXPECT().Kind().Return(Kind("test")).AnyTimes()
	proc.EXPECT().RetryBackOff().Return(time.Duration(10))

	node := models.Node{IP: "1.1.1.1", Port: 8000}
	exec := NewExecutor(context.TODO(), &node, repo)
	exec.Register(proc)
	exec.dispatch(state.EventKeyValue{Key: "xxx", Value: []byte{1, 2, 3}})

	task := Task{State: StateDoneErr}
	exec.dispatch(state.EventKeyValue{Key: "xxx", Value: encoding.JSONMarshal(&task)})

	task = Task{State: StateDoneErr, Kind: "no_kind"}
	exec.dispatch(state.EventKeyValue{Key: "xxx", Value: encoding.JSONMarshal(&task)})

	task = Task{Kind: "test"}
	proc.EXPECT().Process(gomock.Any(), gomock.Any()).Return(nil)
	exec.dispatch(state.EventKeyValue{Key: "xxx", Value: encoding.JSONMarshal(&task)})

	// wait goroutine exit
	time.Sleep(100 * time.Millisecond)

	// after close process fail
	_ = exec.Close()
	task = Task{Kind: "test"}
	exec.dispatch(state.EventKeyValue{Key: "xxx", Value: encoding.JSONMarshal(&task)})
}

func TestExecutor_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repo := state.NewMockRepository(ctrl)

	node := models.Node{IP: "1.1.1.1", Port: 8000}
	exec := NewExecutor(context.TODO(), &node, repo)

	time.AfterFunc(100*time.Millisecond, func() {
		err := exec.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	repo.EXPECT().WatchPrefix(gomock.Any(), gomock.Any(), true).Return(nil)
	exec.Run()
}
