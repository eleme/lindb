package admin

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/lindb/lindb/coordinator"
	"github.com/lindb/lindb/mock"
	"github.com/lindb/lindb/models"
)

type mockIOReader struct {
}

func (m *mockIOReader) Close() error {
	return fmt.Errorf("err")
}
func (m *mockIOReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("err")
}

func TestNewDatabaseFlusherAPI(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	master := coordinator.NewMockMaster(ctrl)
	flushAPI := NewDatabaseFlusherAPI(master)

	// no cluster
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/database/flusher",
		HandlerFunc:    flushAPI.SubmitFlushTask,
		ExpectHTTPCode: http.StatusInternalServerError,
	})

	// no database name
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/database/flush?cluster=test",
		HandlerFunc:    flushAPI.SubmitFlushTask,
		ExpectHTTPCode: http.StatusInternalServerError,
	})

	// submit err
	master.EXPECT().IsMaster().Return(true)
	master.EXPECT().FlushDatabase(gomock.Any(), gomock.Any()).Return(fmt.Errorf("err"))
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/database/flush?cluster=test&db=test",
		HandlerFunc:    flushAPI.SubmitFlushTask,
		ExpectHTTPCode: http.StatusInternalServerError,
	})

	// submit ok
	master.EXPECT().IsMaster().Return(true)
	master.EXPECT().FlushDatabase(gomock.Any(), gomock.Any()).Return(nil)
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/database/flush?cluster=test&db=test",
		HandlerFunc:    flushAPI.SubmitFlushTask,
		ExpectHTTPCode: http.StatusOK,
	})

	defer func() {
		httpGet = http.Get
	}()

	// forward master
	master.EXPECT().IsMaster().Return(false)
	master.EXPECT().GetMaster().Return(&models.Master{
		Node: models.Node{
			IP:   "127.0.0.1",
			Port: 12345,
		},
	})
	httpGet = func(url string) (resp *http.Response, err error) {
		return nil, fmt.Errorf("err")
	}
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/database/flush?cluster=test&db=test",
		HandlerFunc:    flushAPI.SubmitFlushTask,
		ExpectHTTPCode: http.StatusInternalServerError,
	})

	httpGet = func(url string) (resp *http.Response, err error) {
		return nil, nil
	}
	master.EXPECT().IsMaster().Return(false)
	master.EXPECT().GetMaster().Return(&models.Master{
		Node: models.Node{
			IP:   "127.0.0.1",
			Port: 12345,
		},
	})
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/database/flush/ok?cluster=test&db=test",
		HandlerFunc:    flushAPI.SubmitFlushTask,
		ExpectHTTPCode: http.StatusOK,
	})
	httpGet = func(url string) (resp *http.Response, err error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
		}, nil
	}
	master.EXPECT().IsMaster().Return(false)
	master.EXPECT().GetMaster().Return(&models.Master{
		Node: models.Node{
			IP:   "127.0.0.1",
			Port: 12346,
		},
	})
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/database/flush/ok?cluster=test&db=test",
		HandlerFunc:    flushAPI.SubmitFlushTask,
		ExpectHTTPCode: http.StatusInternalServerError,
	})

	httpGet = func(url string) (resp *http.Response, err error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       &mockIOReader{},
		}, nil
	}
	master.EXPECT().IsMaster().Return(false)
	master.EXPECT().GetMaster().Return(&models.Master{
		Node: models.Node{
			IP:   "127.0.0.1",
			Port: 12346,
		},
	})
	mock.DoRequest(t, &mock.HTTPHandler{
		Method:         http.MethodGet,
		URL:            "/database/flush/ok?cluster=test&db=test",
		HandlerFunc:    flushAPI.SubmitFlushTask,
		ExpectHTTPCode: http.StatusOK,
	})
}
