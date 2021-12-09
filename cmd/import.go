// Copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sync"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Imports the docs from the JSON file into the store",
	Long: `Imports the key-values from the specified file (required to be in
JSON format - array of maps - mapping a string to string only),
taking into account - batch size, which can be specified by an
optional flag, into the store. For example:
	./mossScope import <path_to_store> <flag(s)>
Order of execution (if all flags included): stdin < cmdline < file
Expected JSON file format:
	[{"k" : "key0", "v" : "val0"}, {"k" : "key1", "v" : "val1"}]`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("one path needed")
		} else if len(args) != 1 {
			return fmt.Errorf("only one path allowed")
		}

		if len(fileInput) == 0 && len(jsonInput) == 0 && !readFromStdin {
			return fmt.Errorf("at least one input source required")
		}

		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		fromFile := ""
		fromCla := ""
		fromStdin := ""

		var err error

		if len(fileInput) > 0 {
			var input []byte
			input, err = ioutil.ReadFile(fileInput)
			if err != nil {
				return fmt.Errorf("File read error: %v", err)
			}
			fromFile = string(input)
		}

		if len(jsonInput) > 0 {
			fromCla = jsonInput
		}

		if readFromStdin {
			reader := bufio.NewReader(os.Stdin)
			fromStdin, err = reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("Error in reading from stdin, err: %v", err)
			}
		}

		err = invokeImport(fromStdin, args[0])
		if err != nil {
			return fmt.Errorf("Import from STDIN failed; err: %v", err)
		}

		err = invokeImport(fromCla, args[0])
		if err != nil {
			return fmt.Errorf("Import from CMD-LINE failed; err: %v", err)
		}

		err = invokeImport(fromFile, args[0])
		if err != nil {
			return fmt.Errorf("Import from FILE failed; err: %v", err)
		}

		return nil
	},
}

var batchSize int
var fileInput string
var jsonInput string
var readFromStdin bool

type keyVal struct {
	Key string `json:"k"`
	Val string `json:"v"`
}

func invokeImport(jsonStr string, dir string) error {
	if len(jsonStr) == 0 {
		return nil
	}

	input := []byte(jsonStr)

	var data []keyVal
	err := json.Unmarshal(input, &data)
	if err != nil {
		fmt.Printf("Expected format:")
		fmt.Printf("[{\"k\" : \"key0\", \"v\" : \"val0\"}, " +
			"{\"k\" : \"key1\", \"v\" : \"val1\"}]\n")
		return fmt.Errorf("Json-UnMarshal() failed!, err: %v", err)
	}

	if len(data) == 0 {
		fmt.Println("Empty JSON file, no key-values to load!")
		return nil
	}

	if _, err = os.Stat(dir); os.IsNotExist(err) {
		// Create the directory (specified) if it does not already exist
		os.Mkdir(dir, 0777)
	}

	var m sync.Mutex
	var waitingForCleanCh chan struct{}

	var store *moss.Store
	var coll moss.Collection
	var stats *moss.CollectionStats

	co := moss.CollectionOptions{
		OnEvent: func(event moss.Event) {
			if event.Kind == moss.EventKindPersisterProgress {
				stats, err = coll.Stats()
				if err == nil && stats.CurDirtyOps <= 0 &&
					stats.CurDirtyBytes <= 0 && stats.CurDirtySegments <= 0 {
					m.Lock()
					if waitingForCleanCh != nil {
						waitingForCleanCh <- struct{}{}
						waitingForCleanCh = nil
					}
					m.Unlock()
				}
			}
		},
	}

	store, coll, err = moss.OpenStoreCollection(dir,
		moss.StoreOptions{CollectionOptions: co},
		moss.StorePersistOptions{})
	if err != nil || store == nil {
		return fmt.Errorf("Moss-OpenStoreCollection failed, err: %v", err)
	}

	defer store.Close()
	defer coll.Close()

	ch := make(chan struct{}, 1)

	numBatches := 1
	itemsWritten := 0

	if batchSize <= 0 {
		// All key-values in a single batch

		sizeOfBatch := 0
		for i := 0; i < len(data); i++ {
			// Get the size of the batch
			sizeOfBatch += len(data[i].Key) + len(data[i].Val)
		}

		if sizeOfBatch == 0 {
			return nil
		}

		batch, err := coll.NewBatch(len(data), sizeOfBatch)
		if err != nil {
			return fmt.Errorf("Collection-NewBatch() failed, err: %v", err)
		}

		var kbuf, vbuf []byte

		for i := 0; i < len(data); i++ {
			if len(data[i].Key) == 0 {
				continue
			}

			kbuf, err = batch.Alloc(len(data[i].Key))
			if err != nil {
				return fmt.Errorf("Batch-Alloc() failed, err: %v", err)
			}
			vbuf, err = batch.Alloc(len(data[i].Val))
			if err != nil {
				return fmt.Errorf("Batch-Alloc() failed, err: %v", err)
			}

			copy(kbuf, data[i].Key)
			copy(vbuf, data[i].Val)

			err = batch.AllocSet(kbuf, vbuf)
			if err != nil {
				return fmt.Errorf("Batch-AllocSet() failed, err: %v", err)
			}
			itemsWritten++
		}

		m.Lock()
		waitingForCleanCh = ch
		m.Unlock()

		err = coll.ExecuteBatch(batch, moss.WriteOptions{})
		if err != nil {
			return fmt.Errorf("Collection-ExecuteBatch() failed, err: %v", err)
		}

	} else {

		numBatches = int(math.Ceil(float64(len(data)) / float64(batchSize)))
		cursor := 0

		for i := 0; i < numBatches; i++ {
			sizeOfBatch := 0
			numItemsInBatch := 0
			for j := cursor; j < cursor+batchSize; j++ {
				if j >= len(data) {
					break
				}
				sizeOfBatch += len(data[j].Key) + len(data[j].Val)
				numItemsInBatch++
			}
			if sizeOfBatch == 0 {
				continue
			}

			batch, err := coll.NewBatch(numItemsInBatch, sizeOfBatch)
			if err != nil {
				return fmt.Errorf("Collection-NewBatch() failed, err: %v", err)
			}

			var kbuf, vbuf []byte

			for j := 0; j < numItemsInBatch; j++ {
				if len(data[cursor].Key) == 0 {
					cursor++
					continue
				}

				kbuf, err = batch.Alloc(len(data[cursor].Key))
				if err != nil {
					return fmt.Errorf("Batch-Alloc() failed, err: %v", err)
				}
				vbuf, err = batch.Alloc(len(data[cursor].Val))
				if err != nil {
					return fmt.Errorf("Batch-Alloc() failed, err: %v", err)
				}

				copy(kbuf, data[cursor].Key)
				copy(vbuf, data[cursor].Val)

				err = batch.AllocSet(kbuf, vbuf)
				if err != nil {
					return fmt.Errorf("Batch-AllocSet() failed, err: %v", err)
				}
				cursor++
				itemsWritten++
			}

			m.Lock()
			waitingForCleanCh = ch
			m.Unlock()

			err = coll.ExecuteBatch(batch, moss.WriteOptions{})
			if err != nil {
				return fmt.Errorf("Collection-ExecuteBatch() failed, err: %v",
					err)
			}
		}
	}

	<-ch

	fmt.Printf("DONE! .. Wrote %d key-values, in %d batch(es)\n",
		itemsWritten, numBatches)

	return nil
}

func init() {
	RootCmd.AddCommand(importCmd)

	// Local flags that are intended to work with import
	importCmd.Flags().IntVar(&batchSize, "batchsize", 0,
		"Batch-size for the set operations (default: all docs in one batch)")
	importCmd.Flags().StringVar(&fileInput, "file", "",
		"Reads JSON content from file")
	importCmd.Flags().StringVar(&jsonInput, "json", "",
		"Reads JSON content from command-line")
	importCmd.Flags().BoolVar(&readFromStdin, "stdin", false,
		"Reads JSON content from stdin (Enter to submit)")
}
