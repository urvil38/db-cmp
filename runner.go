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
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
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
	doneCh   chan struct{}
	workCh   chan string
	dbWorkCh chan string

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
		doneCh:   make(chan struct{}),
		workCh:   make(chan string),
		mu:       sync.RWMutex{},
		work:     make(map[string]Series),
		vmclient: vmc,
	}, nil
}

func (w *Worker) Start(n int) {
	tenants := []string{"last9"}
	for i := 1; i <= n; i++ {
		go w.Do(i)
	}
	for _, t := range tenants {
		w.workCh <- t
	}
}

func (w *Worker) Do(id int) {
	fmt.Println("starting %v worker", id)
	for t := range w.workCh {

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
		defer w.mu.Unlock()
		w.work[t] = s
		w.dbWorkCh <- t
	}
}

type PostgresWorker struct {
	*Worker
	db *sql.DB
}

func (pw *PostgresWorker) Start(n int) {
	for i :=  1 ; i <= n ; i++ {
		go pw.Do(i)
	}
}

func (mw *MongoWorker) Start(n int) {
	for i :=  1 ; i <= n ; i++ {
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
	fmt.Println("starting pg worker %v", id)
	for t := range pw.workCh {
		var values []interface{}

		pw.mu.RLock()
		defer pw.mu.RUnlock()
		s, ok := pw.work[t]
		if !ok {
			continue
		}
		for _, series := range s {
			var rec LabelRecord
			tenant, ok := series["__tenant__"]
			if !ok {
				continue
			}
			for k, v := range series {
				rec.labels = append(rec.labels, Label{
					Name:  k,
					Value: v,
				})
			}
			rec.tenant = tenant
			rec.hash = CardinalityHash(rec.labels)
			delete(series, "__tenant__")
			delete(series, "le")
			delete(series, "__l9lake__")
			b, err := json.Marshal(series)
			if err != nil {
				continue
			}

			values = append(values, rec.tenant, b, rec.hash, time.Now().Unix(), time.Now().Unix())
		}

		start := time.Now()
		err := postgres.BulkUpsert(context.Background(), pw.db, "tenant_labelset", []string{"tenant", "labels", "cardinality", "created_at", "updated_at"}, values, []string{"tenant", "cardinality"}, []string{"tenant", "cardinality", "labels", "created_at"})
		if err != nil {
			continue
		}
		log.Println("took:", time.Since(start))
	}
	pw.doneCh <- struct{}{}
}

func (mw *MongoWorker) Do(id int) {
	fmt.Println("starting mongo worker: %v", id)
	for t := range mw.workCh {
		mw.mu.RLock()
		defer mw.mu.RUnlock()
		s, ok := mw.work[t]
		if !ok {
			continue
		}

		var models []mongo.WriteModel

		for _, series := range s {
			var rec LabelRecord
			tenant, ok := series["__tenant__"]
			if !ok {
				continue
			}
			for k, v := range series {
				rec.labels = append(rec.labels, Label{
					Name:  k,
					Value: v,
				})
			}
			rec.tenant = tenant
			rec.hash = CardinalityHash(rec.labels)
			delete(series, "__tenant__")
			delete(series, "le")
			delete(series, "__l9lake__")

			bs := make(bson.M)

			for k, v := range series {
				bs[k] = v
			}

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
			models = append(models, mongo.NewUpdateOneModel().SetFilter(bson.D{{"_id", rec.hash}}).
				SetUpdate(mval).SetUpsert(true))

			start1 := time.Now()
			err := mongo_ops.BulkUpsert(mw.db, "last9", models)
			if err != nil {
				continue
			}
			log.Println("took:", time.Since(start1))
		}
	}
	mw.doneCh <- struct{}{}
}
