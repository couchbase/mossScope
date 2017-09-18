// Copyright © 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/couchbase/moss"
)

func TestCompact(t *testing.T) {
	var itemCount = 100
	dir := "testCompactStore"
	os.RemoveAll(dir)
	os.Mkdir(dir, 0777)

	store, err := moss.OpenStore(dir, moss.StoreOptions{})
	if err != nil || store == nil {
		t.Errorf("Expected OpenStore() to work!")
	}

	coll, _ := moss.NewCollection(moss.CollectionOptions{})
	coll.Start()

	for j := 0; j < 3; j++ {
		// Creates
		batch, err2 := coll.NewBatch(itemCount, itemCount*8)
		if err2 != nil {
			t.Errorf("Expected NewBatch() to succeed!")
		}

		for i := 0; i < itemCount; i++ {
			k := []byte(fmt.Sprintf("key%d_%d", i, j))
			v := []byte(fmt.Sprintf("val%d_%d", i, j))
			batch.Set(k, v)
		}

		err2 = coll.ExecuteBatch(batch, moss.WriteOptions{})
		if err2 != nil {
			t.Errorf("Expected ExecuteBatch() to work!")
		}

		ss, _ := coll.Snapshot()

		llss, err3 := store.Persist(ss, moss.StorePersistOptions{})
		if err3 != nil || llss == nil {
			t.Errorf("Expected Persist() to succeed!")
		}

		ss.Close()
	}

	sstats, err := store.Stats()
	if err != nil {
		t.Fatalf("Exepected Stats() to succeed!")
	}
	if sstats["num_segments"].(uint64) <= 1 {
		t.Fatalf("Expected more than 1 segment")
	}
	coll.Close()
	store.Close()
	dirs := []string{dir}

	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err = invokeCompact(dirs)
	if err != nil {
		t.Fatalf("compaction comand failed")
	}

	outC := make(chan string)
	// copy the output in a separate goroutine so dump wouldn't block
	// indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout

	store, err = moss.OpenStore(dir, moss.StoreOptions{})
	if err != nil || store == nil {
		t.Errorf("Expected OpenStore() to work!")
	}

	sstats, err2 := store.Stats()
	if err2 != nil {
		t.Fatalf("Exepected Stats() to succeed!")
	}
	if sstats["num_segments"].(uint64) > 1 {
		t.Fatalf("Expected just 1 segment after compaction")
	}
	store.Close()
}
