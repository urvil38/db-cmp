package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"
)

func main() {
	vmendpoint := flag.String("vm-url", "http://localhost:8456", "vm endpoint")
	//postgresURL := flag.String("pg-url", "postgres://postgres:password@localhost:7432/l9buffer?sslmode=disable", "postgres endpoint")
	mongoURL := flag.String("mongo-url", "mongodb://localhost:27017/?connect=direct", "mongo endpoint")
	workerCount := flag.Int("w", 3, "number of workers to use")
	//memProfile := flag.String("memprof", "mem.pprof", "mem profile")
	flag.Parse()

	w, err := NewWorker(*vmendpoint)
	if err != nil {
		log.Fatal(err)
	}

	w.Start(1)

	mw, err := NewMongoWorker(*mongoURL, w)
	if err != nil {
		log.Fatal(err)
	}

	mw.Start(*workerCount)

	// pw, err := NewPostgresWorker(*postgresURL, w)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// pw.Start(*workerCount)

	for i := 0; i < 10; i++ {
		// if i == 4 {
		// 	f, _ := os.Create(*memProfile)
		// 	pprof.WriteHeapProfile(f)
		// 	f.Close()
		// 	os.Exit(0)
		// }
		fmt.Println("producing work")
		for _, t := range []string{"last9", "thor", "hulk"} {
			w.workCh <- t
		}
		time.Sleep(5 * time.Minute)
		runtime.GC()
	}
	select {}
}
