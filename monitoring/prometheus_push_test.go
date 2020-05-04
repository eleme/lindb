package monitoring

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

var (
	rpcDurations = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "rpc_durations_seconds",
			Help:       "RPC latency distributions.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"service"},
	)
	rpcDurationsHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "rpc_durations_histogram_seconds",
		Help:    "RPC latency distributions.",
		Buckets: DefaultHistogramBuckets,
	})
	rpcCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rpc_counter",
			Help: "RPC counter",
		},
		[]string{"service"},
	)
)

func init() {
	// Register the summary and the histogram with Prometheus's default registry.
	prometheus.MustRegister(rpcDurations)
	prometheus.MustRegister(rpcDurationsHistogram)
	prometheus.MustRegister(rpcCounter)
	// Periodically record some sample latencies for the three services.
	go func() {
		for {
			v := rand.Float64()
			rpcDurations.WithLabelValues("uniform").Observe(v)
			rpcDurationsHistogram.Observe(v)
			rpcCounter.WithLabelValues("uniform").Inc()
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}()
}

func TestPrometheusPusher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = ioutil.ReadAll(r.Body)
		}))
	defer ts.Close()

	pusher := NewPrometheusPusher(
		ctx,
		ts.URL,
		time.Millisecond*100,
		prometheus.Gatherers{prometheus.DefaultGatherer},
		[]*dto.LabelPair{{
			Name:  proto.String("key"),
			Value: proto.String("value"),
		}},
	)
	defer pusher.Stop()

	go pusher.Start()

	time.Sleep(400 * time.Millisecond)
}

func TestPrometheusPusher_push_err(t *testing.T) {
	defer func() {
		newRequest = http.NewRequest
		doRequest = http.DefaultClient.Do
	}()

	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = ioutil.ReadAll(r.Body)
		}))
	defer ts.Close()

	pusher := NewPrometheusPusher(
		context.TODO(),
		ts.URL,
		time.Millisecond*100,
		prometheus.Gatherers{prometheus.DefaultGatherer},
		[]*dto.LabelPair{{
			Name:  proto.String("key"),
			Value: proto.String("value"),
		}},
	)

	c := pusher.(*prometheusPusher)
	// case 1: gather get err
	c.gatherFunc = func(gatherers prometheus.Gatherers) ([]*dto.MetricFamily, error) {
		return nil, fmt.Errorf("err")
	}
	c.run()
	c.gatherFunc = gather

	c.encodeFunc = func(enc expfmt.Encoder, mf *dto.MetricFamily) error {
		return fmt.Errorf("err")
	}
	c.run()
	c.encodeFunc = encode
	// case 3: new request err
	newRequest = func(method, url string, body io.Reader) (request *http.Request, err error) {
		return nil, fmt.Errorf("err")
	}
	c.run()
	newRequest = http.NewRequest
	// case 4: do request err
	doRequest = func(req *http.Request) (response *http.Response, err error) {
		return nil, fmt.Errorf("err")
	}
	c.run()
}
