package state

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/lindb/lindb/coordinator/broker"
	"github.com/lindb/lindb/mock"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/pkg/state"
	"github.com/lindb/lindb/pkg/timeutil"
)

func TestBrokerAPI_ListBrokersStat(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := state.NewMockRepository(ctrl)
	stateMachine := broker.NewMockNodeStateMachine(ctrl)
	api := NewBrokerAPI(context.TODO(), repo, stateMachine)

	// get stat list err
	repo.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("err"))
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/broker/state",
		HandlerFunc:    api.ListBrokersStat,
		ExpectHTTPCode: 500,
	})

	// decoding stat err
	repo.EXPECT().List(gomock.Any(), gomock.Any()).Return([]state.KeyValue{
		{
			Key:   "/test/1.1.1.1:2080",
			Value: []byte{1, 2, 3},
		},
	}, nil)
	node := models.ActiveNode{Node: models.Node{IP: "1.1.1.1", Port: 2080}, OnlineTime: timeutil.Now()}
	nodes := []models.ActiveNode{node}
	stateMachine.EXPECT().GetActiveNodes().Return(nodes)
	system := models.SystemStat{
		CPUs: 100,
	}
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/broker/state",
		HandlerFunc:    api.ListBrokersStat,
		ExpectHTTPCode: 500,
	})

	// success
	stateMachine.EXPECT().GetActiveNodes().Return(nodes)
	repo.EXPECT().List(gomock.Any(), gomock.Any()).Return([]state.KeyValue{
		{
			Key: "/test/1.1.1.1:2080",
			Value: encoding.JSONMarshal(&models.NodeStat{
				Node:   node,
				System: system,
			}),
		},
		{
			Key: "/test/1.1.1.2:2080",
			Value: encoding.JSONMarshal(&models.NodeStat{
				Node:   node,
				System: system,
			}),
		},
	}, nil)
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/broker/state",
		HandlerFunc:    api.ListBrokersStat,
		ExpectHTTPCode: 200,
		ExpectResponse: []models.NodeStat{{
			Node:   node,
			System: system,
		}, {
			Node:   node,
			System: system,
			IsDead: true,
		}},
	})
}
