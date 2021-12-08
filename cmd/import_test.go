// Copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/couchbase/moss"
)

func importHelper(t *testing.T, batchsize int) {
	// Create a JSON file with some sample content
	jsonText := "[{\"k\":\"key0\",\"v\":\"val0\" }," +
		"{\"k\":\"key1\",\"v\":\"val1\"}," +
		"{\"k\":\"key2\",\"v\":\"val2\"}," +
		"{\"k\":\"key3\",\"v\":\"val3\"}," +
		"{\"k\":\"key4\",\"v\":\"val4\"}," +
		"{\"k\":\"key5\",\"v\":\"val5\"}," +
		"{\"k\":\"key6\",\"v\":\"val6\"}," +
		"{\"k\":\"key7\",\"v\":\"val7\"}," +
		"{\"k\":\"key8\",\"v\":\"val8\"}," +
		"{\"k\":\"key9\",\"v\":\"val9\"}]"

	tempDir := "testImportStore"

	// Prevent the command from writing anything to stdout
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	batchSize = batchsize
	err := invokeImport(jsonText, tempDir)
	if err != nil {
		t.Error(err)
	}

	outC := make(chan string)
	// copy the output in a separate goroutine so dump wouldn't
	// block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	<-outC

	defer os.RemoveAll(tempDir)

	store, err := moss.OpenStore(tempDir, moss.StoreOptions{})
	if err != nil || store == nil {
		t.Errorf("Expected OpenStore() to work!")
	}
	defer store.Close()

	snapshot, _ := store.Snapshot()
	defer snapshot.Close()

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("val%d", i)
		val, err := snapshot.Get([]byte(k), moss.ReadOptions{})
		if err != nil {
			t.Errorf("Expected Snapshot-Get() to succeed!")
		}

		if len(val) == len(v) {
			for j := range v {
				if val[j] != v[j] {
					t.Errorf("Value mismatch!")
				}
			}
		} else {
			t.Errorf("Value length mismatch!")
		}
	}
}

func TestImport(t *testing.T) {
	importHelper(t, 0)
}

func TestImportWithBatchSize(t *testing.T) {
	importHelper(t, 3)
}
