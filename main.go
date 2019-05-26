package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	gokitlog "github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

func main() {
	err := _main()
	if err != nil {
		log.Fatalf("[ERROR] %s", err)
	}
}

func _main() error {
	logger := gokitlog.NewLogfmtLogger(os.Stderr)

	inputPath := flag.String("input", "", "path to input block")
	outputDir := flag.String("output", "", "path to output blocks dir")
	intervalStr := flag.String("interval", "", "sampling interval")
	flag.Parse()

	interval, err := time.ParseDuration(*intervalStr)
	if err != nil {
		return errors.Wrap(err, "parsing interval")
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)
	outputPath := filepath.Join(*outputDir, uid.String())

	log.Printf("[INFO] Downsampling a block at %s", *inputPath)

	err = os.Mkdir(outputPath, 0777)
	if err != nil {
		return errors.Wrap(err, "os.Mkdir")
	}

	block, err := tsdb.OpenBlock(logger, *inputPath, chunkenc.NewPool())
	if err != nil {
		return errors.Wrap(err, "open block")
	}
	defer block.Close()

	indexr, err := block.Index()
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer indexr.Close()

	indexw, err := index.NewWriter(filepath.Join(outputPath, "index"))
	if err != nil {
		return errors.Wrap(err, "open index writer")
	}
	defer indexw.Close()

	chunkr, err := block.Chunks()
	if err != nil {
		return errors.Wrap(err, "open chunk reader")
	}
	defer chunkr.Close()

	postings, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		return errors.Wrap(err, "list all postings")
	}

	chunkw, err := chunks.NewWriter(filepath.Join(outputPath, "chunks"))
	if err != nil {
		return errors.Wrap(err, "create a chunk writer")
	}

	symbols, err := indexr.Symbols()
	if err != nil {
		return errors.Wrap(err, "get symbols in an index")
	}

	err = indexw.AddSymbols(symbols)
	if err != nil {
		return errors.Wrap(err, "add symbols in an index")
	}

	var globalMaxTime int64

	toPostings := index.NewMemPostings()
	for postings.Next() {
		indexRef := postings.At()

		lset := labels.Labels{}
		chks := []chunks.Meta{}
		err = indexr.Series(indexRef, &lset, &chks)
		if err != nil {
			return errors.Wrap(err, "get a series")
		}

		toPostings.Add(indexRef, lset)

		// log.Printf("labels: %+v, chunks: %+v", lset, chks)

		newChunk := chunkenc.NewXORChunk()
		chunkAppender, err := newChunk.Appender()
		if err != nil {
			return errors.Wrap(err, "create a chunk appender")
		}

		var maxTime int64
		var minTime int64
		for _, chk := range chks {
			c, err := chunkr.Chunk(chk.Ref)
			if err != nil {
				return errors.Wrap(err, "get a chunk")
			}

			iter := c.Iterator()
			for iter.Next() {
				t, v := iter.At()
				if maxTime == 0 || maxTime+interval.Nanoseconds()/1000/1000 <= t {
					chunkAppender.Append(t, v)
					// log.Printf("t:%d, v:%f", t, v)
					maxTime = t
					if minTime == 0 {
						minTime = t
					}
					if globalMaxTime < t {
						globalMaxTime = t
					}
				}
			}
			if err := iter.Err(); err != nil {
				return errors.Wrap(err, "iterate a chunk")
			}
		}

		m := []chunks.Meta{{
			MinTime: minTime,
			MaxTime: maxTime,
			Chunk:   newChunk,
		}}
		err = chunkw.WriteChunks(m...)
		if err != nil {
			return errors.Wrap(err, "write a chunk")
		}

		err = indexw.AddSeries(indexRef, lset, m...)
		if err != nil {
			return errors.Wrap(err, "write a series to an index")
		}
	}
	if err := postings.Err(); err != nil {
		return errors.Wrap(err, "iterate postings")
	}

	if err := chunkw.Close(); err != nil {
		return errors.Wrap(err, "close a chunk writer")
	}

	sortedKeys := toPostings.SortedKeys()

	var name string
	values := []string{}
	for _, l := range sortedKeys {
		if l.Name == "" && l.Value == "" {
			continue
		}
		if name == "" { // first time
			name = l.Name
		}
		if l.Name != name && len(values) > 0 {
			indexw.WriteLabelIndex([]string{name}, values)
			name = l.Name
			values = []string{}
		}
		values = append(values, l.Value)
	}
	if len(values) > 0 {
		indexw.WriteLabelIndex([]string{name}, values)
	}

	for _, l := range sortedKeys {
		err := indexw.WritePostings(l.Name, l.Value, toPostings.Get(l.Name, l.Value))
		if err != nil {
			return errors.Wrap(err, "writer.WritePostings")
		}
	}

	meta := block.Meta()
	meta.ULID = uid
	meta.MaxTime = globalMaxTime
	meta.Stats = tsdb.BlockStats{}
	b, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "json.Marshal")
	}
	err = ioutil.WriteFile(filepath.Join(outputPath, "meta.json"), b, 0666)
	if err != nil {
		return errors.Wrap(err, "ioutil.WriteFile")
	}

	log.Printf("[INFO] Downsampling completed. A block has been created at %s", outputPath)

	return nil
}
