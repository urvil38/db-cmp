package main

import (
	"flag"
	"log"
	"runtime"
)

func main() {
	vmendpoint := flag.String("vm-url", "http://localhost:8456", "vm endpoint")
	postgresURL := flag.String("pg-url", "postgres://postgres:password@localhost:7432/l9buffer?sslmode=disable", "postgres endpoint")
	mongoURL := flag.String("mongo-url", "mongodb://localhost:27017/?connect=direct", "mongo endpoint")
	workerCount := flag.Int("w", runtime.NumCPU(), "number of workers to use")

	w, err := NewWorker(*vmendpoint)
	if err != nil {
		log.Fatal(err)
	}

	w.Start(*workerCount)

	mw, err := NewMongoWorker(*mongoURL, w)
	if err != nil {
		log.Fatal(err)
	}

	mw.Start(*workerCount)

	pw, err := NewPostgresWorker(*postgresURL, w)
	if err != nil {
		log.Fatal(err)
	}

	pw.Start(*workerCount)

	select {}
}
