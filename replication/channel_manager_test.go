package replication

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/config"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/fileutil"
	"github.com/lindb/lindb/pkg/ltoml"
)

var replicationConfig = config.ReplicationChannel{
	Dir:                "/tmp/broker/replication",
	SegmentFileSize:    uint16(128),
	RemoveTaskInterval: ltoml.Duration(time.Minute),
	ReportInterval:     ltoml.Duration(time.Second),
	FlushInterval:      ltoml.Duration(0),
	CheckFlushInterval: ltoml.Duration(100 * time.Millisecond),
	BufferSize:         2,
}

func TestChannelManager_GetChannel(t *testing.T) {
	ctrl := gomock.NewController(t)
	dirPath := path.Join(os.TempDir(), "test_channel_manager")
	defer func() {
		if err := os.RemoveAll(dirPath); err != nil {
			t.Error(err)
		}
		ctrl.Finish()
	}()

	replicatorStateReport := NewMockReplicatorStateReport(ctrl)
	replicatorStateReport.EXPECT().Report(gomock.Any()).Return(fmt.Errorf("err")).AnyTimes()

	replicationConfig.Dir = dirPath
	cm := NewChannelManager(replicationConfig, nil, replicatorStateReport)

	_, err := cm.CreateChannel("database", 2, 2)
	assert.Error(t, err)

	ch1, err := cm.CreateChannel("database", 3, 0)
	assert.NoError(t, err)

	ch111, err := cm.CreateChannel("database", 3, 0)
	assert.NoError(t, err)
	assert.Equal(t, ch111, ch1)

	defer func() {
		mkdir = fileutil.MkDirIfNotExist
	}()
	mkdir = func(path string) error {
		return fmt.Errorf("err")
	}
	_, err = cm.CreateChannel("database-err", 3, 1)
	assert.Error(t, err)

	cm1 := cm.(*channelManager)
	cm1.databaseChannelMap.Store("database-value-err", "test")
	c, ok := cm1.getDatabaseChannel("database-value-err")
	assert.False(t, ok)
	assert.Nil(t, c)

	cm.Close()
}

func TestChannelManager_Write(t *testing.T) {
	ctrl := gomock.NewController(t)
	dirPath := path.Join(os.TempDir(), "test_channel_manager")
	defer func() {
		if err := os.RemoveAll(dirPath); err != nil {
			t.Error(err)
		}
		ctrl.Finish()
	}()

	replicatorStateReport := NewMockReplicatorStateReport(ctrl)
	replicatorStateReport.EXPECT().Report(gomock.Any()).Return(fmt.Errorf("err")).AnyTimes()

	replicationConfig.Dir = dirPath
	cm := NewChannelManager(replicationConfig, nil, replicatorStateReport)
	err := cm.Write("database", nil)
	assert.Error(t, err)

	dbChannel := NewMockDatabaseChannel(ctrl)
	cm1 := cm.(*channelManager)
	cm1.databaseChannelMap.Store("database", dbChannel)
	dbChannel.EXPECT().Write(gomock.Any()).Return(nil)
	err = cm.Write("database", nil)
	assert.NoError(t, err)
	cm.Close()
}

func TestChannelManager_ReportState(t *testing.T) {
	ctrl := gomock.NewController(t)
	dirPath := path.Join(os.TempDir(), "test_channel_manager")
	defer func() {
		if err := os.RemoveAll(dirPath); err != nil {
			t.Error(err)
		}
		ctrl.Finish()
	}()

	replicatorStateReport := NewMockReplicatorStateReport(ctrl)
	replicatorStateReport.EXPECT().Report(gomock.Any()).Return(fmt.Errorf("err")).AnyTimes()

	replicationConfig.Dir = dirPath
	cm := NewChannelManager(replicationConfig, nil, replicatorStateReport)
	time.Sleep(2 * time.Second)
	cm.Close()
	// waiting close complete
	time.Sleep(400 * time.Millisecond)

	dbChannel := NewMockDatabaseChannel(ctrl)
	cm1 := cm.(*channelManager)
	cm1.databaseChannelMap.Store("database", dbChannel)
	dbChannel.EXPECT().ReplicaState().Return([]models.ReplicaState{{}}).AnyTimes()
	cm1.reportState()
}
