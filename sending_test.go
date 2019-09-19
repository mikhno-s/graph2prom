package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

func getMetrics(port int) (string, error) {
	host := strings.Join([]string{"http://127.0.0.1:", strconv.Itoa(port), "/metrics"}, "")
	resp, err := http.Get(host)
	if err != nil {
		return "", err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func sendMetrics(m string, port int) error {
	host := strings.Join([]string{"127.0.0.1:", strconv.Itoa(port)}, "")
	addr, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}

	_, err = conn.Write([]byte(m))
	defer conn.Close()
	return err
}

func TestMetricsReceiving(t *testing.T) {

	// Init
	rand.Seed(time.Now().UTC().UnixNano())
	promPort := 31000 + rand.Intn(999)
	graphPort := 32000 + rand.Intn(999)

	c := newMetricsCollector(graphPort)
	prometheus.MustRegister(c)

	wg := &sync.WaitGroup{}
	stop := make(chan bool)
	srv := startPrometheusEndpoint(":"+strconv.Itoa(promPort), stop)

	// waiting for opening port
	for i := 0; i < 5; i++ {
		_, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(graphPort))
		if err != nil {
			time.Sleep(time.Second * 1)
			fmt.Println(i)
		}
		if err == nil {
			break
		}
	}

	t.Run("Sending fedops_events metric", func(t *testing.T) {
		wg.Add(1)
		sec := strconv.Itoa(int(time.Now().Unix()))
		metric := "metric_name.label=blabla 6666" + sec
		result := "metric_name{label=\"blabla\"} 6666"
		err := sendMetrics(metric, graphPort)
		if err != nil {
			t.Error("Sending metrics error", err)
		}
		time.Sleep(time.Second * 1)

		m, err := getMetrics(promPort)
		if err != nil {
			t.Error(err)
		}
		if !strings.Contains(m, result) {
			t.Error("Response body:", m)
			t.Error("Metric was not received")
		}
		if strings.Contains(m, result) {
			fmt.Println(t.Name() + " PASSED")
		}
		wg.Done()
	})

	wg.Wait()
	// Stop servers
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Error(err)
	} else {
		log.Infoln("Prometheus endpoint stopped")
	}
}
