package main

import (
	"context"
	"database/sql"
	"db-cmp/mongo_ops"
	"db-cmp/postgres"
	"db-cmp/vmclient"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Series []map[string]string

var xxHashPool = sync.Pool{
	New: func() interface{} {
		return xxhash.New()
	},
}

type Mongo struct {
	_id        primitive.ObjectID `bson:"_id"`
	labels     map[string]string
	created_at time.Time
	updated_at time.Time
}

type Label struct {
	Name  string
	Value string
}

type LabelSet []LabelRecord

type LabelRecord struct {
	tenant string
	hash   string
	labels []Label
}

// putXXHash is just a wrapper around hashDigest.Reset and put it back.
// Use this over accessing the pool's inbuilt Put method.
func putXXHash(h *xxhash.Digest) {
	h.Reset()
	xxHashPool.Put(h)
}

func CardinalityHash(labels []Label) string {

	sort.Slice(labels[:], func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	ds := xxHashPool.Get().(*xxhash.Digest)
	defer putXXHash(ds)

	for _, o := range labels {
		ds.WriteString(o.Name)
		ds.WriteString(o.Value)
	}

	return strconv.FormatUint(ds.Sum64(), 10)
}

type Worker struct {
	doneCh      chan struct{}
	workCh      chan string
	pgWorkCh    chan string
	mongoWorkCh chan string

	mu       sync.RWMutex
	work     map[string]Series
	vmclient *vmclient.Client
}

func NewWorker(vmaddr string) (*Worker, error) {
	vmc, err := vmclient.NewClient(vmaddr)
	if err != nil {
		return nil, err
	}
	return &Worker{
		doneCh:      make(chan struct{}),
		workCh:      make(chan string),
		pgWorkCh:    make(chan string),
		mongoWorkCh: make(chan string),
		mu:          sync.RWMutex{},
		work:        make(map[string]Series),
		vmclient:    vmc,
	}, nil
}

func (w *Worker) Start(n int) {
	for i := 1; i <= n; i++ {
		go w.Do(i)
	}
}

func (w *Worker) Do(id int) {
	fmt.Println("starting worker: ", id)
	for t := range w.workCh {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		queryParams := map[string]string{
			"match[]": fmt.Sprintf("{__tenant__=%q}", t),
			"start":   "-10m",
		}
		res, err := w.vmclient.Series(ctx, queryParams)
		if err != nil {
			log.Println(err)
			return
		}
		var s Series
		err = json.Unmarshal(res, &s)
		if err != nil {
			log.Println(err)
			return
		}
		w.mu.Lock()
		w.work[t] = s
		w.mu.Unlock()
		w.mongoWorkCh <- t
		//w.pgWorkCh <- t
		res = nil
		fmt.Println("took: ", time.Since(start))
	}
}

type PostgresWorker struct {
	*Worker
	db *sql.DB
}

func (pw *PostgresWorker) Start(n int) {
	for i := 1; i <= n; i++ {
		go pw.Do(i)
	}
}

func (mw *MongoWorker) Start(n int) {
	for i := 1; i <= n; i++ {
		go mw.Do(i)
	}
}

func NewPostgresWorker(dbURL string, w *Worker) (*PostgresWorker, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("aborting startup due to error %v", err.Error())
	}
	return &PostgresWorker{
		db:     db,
		Worker: w,
	}, nil
}

type MongoWorker struct {
	*Worker
	db *mongo.Client
}

func NewMongoWorker(dbURL string, w *Worker) (*MongoWorker, error) {
	clientOpts := options.Client().ApplyURI(dbURL)
	client, err := mongo.Connect(context.TODO(), clientOpts)
	if err != nil {
		return nil, err
	}
	return &MongoWorker{
		db:     client,
		Worker: w,
	}, nil
}

func (pw *PostgresWorker) Do(id int) {
	fmt.Println("starting pg worker ", id)
	for t := range pw.pgWorkCh {
		fmt.Println("pg ", t)

		pw.mu.RLock()
		s, ok := pw.work[t]
		if !ok {
			pw.mu.RUnlock()
			continue
		}
		pw.mu.RUnlock()

		values := make([]interface{}, len(s)*5)
		var i int
		for _, series := range s {
			var rec LabelRecord
			_, ok := series["__tenant__"]
			if !ok {
				continue
			}

			labels := make(map[string]string)
			for k, v := range series {
				rec.labels = append(rec.labels, Label{
					Name:  k,
					Value: v,
				})
				if k == "__tenant__" || k == "__l9lake__" {
					continue
				}
				labels[k] = v
			}
			rec.tenant = t
			rec.hash = CardinalityHash(rec.labels)
			b, _ := json.Marshal(labels)
			values[i] = rec.tenant
			values[i+1] = b
			values[i+2] = rec.hash
			values[i+3] = time.Now().Unix()
			values[i+4] = time.Now().Unix()
			i += 5
			//values = append(values, rec.tenant, string(b), rec.hash, time.Now().Unix(), time.Now().Unix())
		}

		start := time.Now()
		err := postgres.BulkUpsert(context.Background(), pw.db, "tenant_labelset", []string{"tenant", "labels", "cardinality", "created_at", "updated_at"}, values, []string{"tenant", "cardinality"}, []string{"tenant", "cardinality", "labels", "created_at"})
		if err != nil {
			fmt.Println(err)
			continue
		}
		values = nil
		log.Println("pg took:", time.Since(start))
	}
	pw.doneCh <- struct{}{}
}

func (mw *MongoWorker) Do(id int) {
	fmt.Println("starting mongo worker: ", id)
	for t := range mw.mongoWorkCh {
		fmt.Println("mongo ", t)
		mw.mu.RLock()
		s, ok := mw.work[t]
		if !ok {
			mw.mu.RUnlock()
			continue
		}
		mw.mu.RUnlock()

		//var models []mongo.WriteModel
		offset := 10000
		total := len(s)
		start1 := time.Now()
		for left := 0; left < total; left += offset {
			right := left + offset
			if right > total {
				right = total
			}
			models := make([]mongo.WriteModel, right-left)
			for i, series := range s[left:right] {
				var rec LabelRecord
				_, ok := series["__tenant__"]
				if !ok {
					continue
				}

				bs := make(bson.M)
				for k, v := range series {
					rec.labels = append(rec.labels, Label{
						Name:  k,
						Value: v,
					})
					if k == "__tenant__" || k == "__l9lake__" {
						continue
					}
					bs[k] = v
				}
				rec.tenant = t
				rec.hash = CardinalityHash(rec.labels)

				mval := bson.D{
					{"$set", bson.D{
						{"updated_at", time.Now()},
					}},
					{"$setOnInsert", bson.D{
						{"_id", rec.hash},
						{"created_at", time.Now()},
						{"labels", bs},
					}},
				}
				models[i] = mongo.NewUpdateOneModel().SetFilter(bson.D{{"_id", rec.hash}}).
					SetUpdate(mval).SetUpsert(true)
				// models = append(models, mongo.NewUpdateOneModel().SetFilter(bson.D{{"_id", rec.hash}}).
				// 	SetUpdate(mval).SetUpsert(true))
			}

			err := mongo_ops.BulkUpsert(mw.db, t, models)
			if err != nil {
				fmt.Println(err)
				continue
			}
			models = nil
		}
		log.Println("mongo took:", time.Since(start1))
	}
	mw.doneCh <- struct{}{}
}
