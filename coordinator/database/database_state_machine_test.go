package database

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/coordinator/discovery"
	"github.com/lindb/lindb/models"
)

func TestNewDBStateMachine(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := discovery.NewMockFactory(ctrl)
	discovery1 := discovery.NewMockDiscovery(ctrl)
	factory.EXPECT().CreateDiscovery(gomock.Any(), gomock.Any()).Return(discovery1)
	discovery1.EXPECT().Discovery().Return(fmt.Errorf("err"))
	_, err := NewDBStateMachine(context.TODO(), factory)
	assert.Error(t, err)

	// normal case
	factory.EXPECT().CreateDiscovery(gomock.Any(), gomock.Any()).Return(discovery1)
	discovery1.EXPECT().Discovery().Return(nil)
	stateMachine, err := NewDBStateMachine(context.TODO(), factory)
	assert.NoError(t, err)
	assert.NotNil(t, stateMachine)
}

func TestDBStateMachine_listen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := discovery.NewMockFactory(ctrl)
	discovery1 := discovery.NewMockDiscovery(ctrl)
	// normal case
	factory.EXPECT().CreateDiscovery(gomock.Any(), gomock.Any()).Return(discovery1)
	discovery1.EXPECT().Discovery().Return(nil)
	stateMachine, err := NewDBStateMachine(context.TODO(), factory)
	assert.NoError(t, err)

	db := models.Database{Name: "test"}
	data, _ := json.Marshal(&db)
	stateMachine.OnCreate("/data/test", data)

	db2, ok := stateMachine.GetDatabaseCfg("test")
	assert.True(t, ok)
	assert.Equal(t, db, db2)

	stateMachine.OnCreate("/data/test2", []byte{1, 1})
	_, ok = stateMachine.GetDatabaseCfg("test2")
	assert.False(t, ok)

	data, _ = json.Marshal(&models.Database{})
	stateMachine.OnCreate("/data/test2", data)
	_, ok = stateMachine.GetDatabaseCfg("test2")
	assert.False(t, ok)

	stateMachine.OnDelete("/data/test")
	_, ok = stateMachine.GetDatabaseCfg("test")
	assert.False(t, ok)

	discovery1.EXPECT().Close()
	_ = stateMachine.Close()
}
