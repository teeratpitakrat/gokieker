package gokieker

import (
	"encoding/json"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

const (
	MyDB     = "kieker"
	username = "root"
	password = "root"
)

var writeChannel chan MonitoringRecord = make(chan MonitoringRecord)
var hostname string

type MonitoringRecord interface {
	JSON() ([]byte, error)
	MapFields() map[string]interface{}
	MapTags() map[string]string
}

type OperationExecutionRecord struct {
	OperationSignature string `json:"operation_signature"`
	SessionId          string `json:"session_id"`
	TraceId            int64  `json:"trace_id"`
	Hostname           string `json:"hostname"`
	Tin                int64  `json:"tin"`
	Tout               int64  `json:"tout"`
	Eoi                int    `json:"eoi"`
	Ess                int    `json:"ess"`
}

func (r *OperationExecutionRecord) JSON() ([]byte, error) {
	return json.Marshal(r)
}

func (r *OperationExecutionRecord) MapFields() map[string]interface{} {
	fields := map[string]interface{}{
		"operation_signature": r.OperationSignature,
		"session_id":          r.SessionId,
		"trace_id":            r.TraceId,
		"hostname":            r.Hostname,
		"tin":                 r.Tin,
		"tout":                r.Tout,
		"response_time":       r.Tout - r.Tin,
	}
	return fields
}

func (r *OperationExecutionRecord) MapTags() map[string]string {
	tags := map[string]string{
		"operation_signature": r.OperationSignature,
		"hostname":            r.Hostname,
	}
	return tags
}

func StartMonitoring() {
	hostname, _ = os.Hostname()
	// Connect to influxdb
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://influxdb:8086",
		Username: username,
		Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  MyDB,
			Precision: "ns",
		})
		if err != nil {
			log.Fatal(err)
		}
		lastPushTime := time.Unix(0, 0)
		//for r := range writeChannel {
		var r MonitoringRecord
		for {
			select {
			case r = <-writeChannel:
				// Create a new point batch

				// Create a point and add to batch
				tags := r.MapTags()
				fields := r.MapFields()

				pt, err := client.NewPoint("operation_execution", tags, fields, time.Now())
				if err != nil {
					log.Fatal(err)
				}
				bp.AddPoint(pt)

				if (len(bp.Points()) >= 1000) || (time.Since(lastPushTime) >= 100*time.Millisecond) {
					// Write the batch
					if err := c.Write(bp); err != nil {
						log.Fatal(err)
					}
					lastPushTime = time.Now()
					bp, err = client.NewBatchPoints(client.BatchPointsConfig{
						Database:  MyDB,
						Precision: "ns",
					})
					if err != nil {
						log.Fatal(err)
					}
				}
			default:
				time.Sleep(50 * time.Millisecond)
				if (len(bp.Points()) >= 1) && (time.Since(lastPushTime) >= 100*time.Millisecond) {
					// Write the batch
					if err := c.Write(bp); err != nil {
						log.Fatal(err)
					}
					lastPushTime = time.Now()
					bp, err = client.NewBatchPoints(client.BatchPointsConfig{
						Database:  MyDB,
						Precision: "ns",
					})
					if err != nil {
						log.Fatal(err)
					}
				}
			}
		}
	}()
}

func BeginFunction() *OperationExecutionRecord {
	// Get function name
	pc := make([]uintptr, 2)
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])

	r := OperationExecutionRecord{}
	r.OperationSignature = f.Name()
	r.Hostname = hostname
	r.Tin = time.Now().UnixNano()
	return &r
}

func (r *OperationExecutionRecord) EndFunction() {
	r.Tout = time.Now().UnixNano()
	writeChannel <- r
}
