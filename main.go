package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
)

var (
	lastProcessed = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "graphite_last_processed_timestamp_seconds",
			Help: "Unix timestamp of the last processed graphite metric.",
		},
	)
)

type metricsCollector struct {
	lineCh   chan string
	sampleCh chan *graphiteSample
	samples  map[string]*graphiteSample
	mu       *sync.Mutex
	port     int
}

type graphiteSample struct {
	OriginalName string
	Name         string
	Labels       map[string]string
	Value        float64
	Type         prometheus.ValueType
	Help         string
	Timestamp    time.Time
}

func newMetricsCollector(port int) *metricsCollector {
	c := &metricsCollector{
		lineCh:   make(chan string),
		sampleCh: make(chan *graphiteSample),
		samples:  map[string]*graphiteSample{},
		mu:       &sync.Mutex{},
		port:     port,
	}
	go c.processLines()
	go c.processSamples()
	go c.startTCPListener()
	return c
}

func (c *metricsCollector) startTCPListener() {
	tcpSock, err := net.Listen("tcp", ":"+strconv.Itoa(c.port))
	if err != nil {
		log.Fatalf("Error metrics collecto binding to TCP socket: %s", err)
	}
	log.Infof("Started metrics collector on :%d", c.port)
	for {
		conn, err := tcpSock.Accept()
		if err != nil {
			log.Errorf("Error metrics collecto accepting TCP connection: %s", err)
			continue
		}
		go func() {
			defer conn.Close()
			c.readFromConn(conn)
		}()
	}
}

func (c *metricsCollector) readFromConn(r io.Reader) {
	line := bufio.NewScanner(r)
	for {
		if ok := line.Scan(); !ok {
			break
		}
		c.lineCh <- line.Text()
	}
}

func (c *metricsCollector) processLines() {
	for l := range c.lineCh {
		c.processLine(l)

	}
}

func (c *metricsCollector) processLine(l string) {
	l = strings.TrimSpace(l)
	parts := strings.Split(l, " ")
	if len(parts) != 3 {
		log.Infof("Invalid part count of %d in line: %s", len(parts), l)
		return
	}

	value, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		log.Infof("Invalid value in line: %s", l)
		return
	}

	ts, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		log.Infof("Invalid timestamp in line: %s", l)
		return
	}

	if strings.Split(parts[0], ".")[0] == "" {
		return
	}

	labels := make(map[string]string)

	for _, p := range strings.Split(parts[0], ".")[1:] {
		kv := strings.Split(p, "=")
		labels[kv[0]] = kv[1]
	}

	var name string

	switch labels["agg_type"] {
	default:
		name = "graphite_metric"
	case "p95":
		name = "graphite_metric_p95"
	case "p50":
		name = "graphite_metric_p50"
	}

	if name == "" {
		return
	}

	sample := graphiteSample{
		OriginalName: parts[0],
		Name:         name,
		Value:        value,
		Labels:       labels,
		Type:         prometheus.GaugeValue,
		Help:         fmt.Sprintf("Graphite metric %s", name),
		Timestamp:    time.Unix(int64(ts), int64(math.Mod(ts, 1.0)*1e9)),
	}

	lastProcessed.Set(float64(time.Now().UnixNano()) / 1e9)

	c.sampleCh <- &sample

}

func (c *metricsCollector) processSamples() {
	ticker := time.NewTicker(time.Minute).C

	for {
		select {
		case sample, ok := <-c.sampleCh:
			if sample == nil || !ok {
				return
			}
			c.mu.Lock()
			c.samples[sample.OriginalName] = sample
			c.mu.Unlock()

		case <-ticker:
			// Garbage collect expired samples.
			ageLimit := time.Now().Add(-time.Minute * 5)
			c.mu.Lock()
			for k, sample := range c.samples {
				if ageLimit.After(sample.Timestamp) {
					delete(c.samples, k)
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c metricsCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- lastProcessed

	c.mu.Lock()
	samples := make([]*graphiteSample, 0, len(c.samples))
	for _, sample := range c.samples {
		samples = append(samples, sample)
	}
	c.mu.Unlock()

	ageLimit := time.Now().Add(-time.Minute * 5)
	for _, sample := range samples {
		if ageLimit.After(sample.Timestamp) {
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(sample.Name, sample.Help, []string{}, sample.Labels),
			sample.Type,
			sample.Value,
		)
	}
}

// Describe implements prometheus.Collector.
func (c metricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- lastProcessed.Desc()
}

func startPrometheusEndpoint(p string, stop chan bool) *http.Server {
	http.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: p}
	go func() {
		log.Infoln("Started prometheus endpoint on", p)
		log.Info(srv.ListenAndServe())
		close(stop)
	}()
	return srv
}

// func init() {
// 	prometheus.MustRegister(version.NewCollector("graphite2prom"))
// }

func main() {
	c := newMetricsCollector(9091)
	prometheus.MustRegister(c)

	stop := make(chan bool)
	srv := startPrometheusEndpoint(":8080", stop)

	<-stop
	ctx := context.Background()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Error: %v\n", err)
	} else {
		log.Infoln("Prometheus endpoint stopped")
	}
}
