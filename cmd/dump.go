// Copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cmd

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dumps key/val data in the specified store",
	Long: `Dumps every key-value persisted in the store in JSON
format. It has a set of options that it can used with.
For example:
	./mossScope dump [sub-command] <path_to_store> [flag]`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("at least one path is required")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeDump(args)
	},
}

var keysOnly bool
var inHex bool

func invokeDump(dirs []string) error {
	fmt.Printf("[")
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, readOnlyMode)
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}

		snap, err := store.Snapshot()
		if err != nil || snap == nil {
			return fmt.Errorf("Store-Snapshot() API failed, err: %v", err)
		}

		iter, err := snap.StartIterator(nil, nil, moss.IteratorOptions{})
		if err != nil || iter == nil {
			return fmt.Errorf("Snapshot-StartItr() API failed, err: %v", err)
		}

		if index != 0 {
			fmt.Printf(",")
		}
		fmt.Printf("{\"%s\":", dir)

		fmt.Printf("[")
		for err, firstDoc := error(nil), true; err == nil; err = iter.Next() {
			var k, v []byte
			k, v, err = iter.Current()
			if err != nil {
				break
			}

			if keyPrefix != "" && !strings.HasPrefix(string(k), keyPrefix) {
				continue
			}
			if keysOnly {
				err = dumpKeyVal(k, nil, inHex, &firstDoc)
			} else {
				err = dumpKeyVal(k, v, inHex, &firstDoc)
			}

			if err != nil {
				return err
			}
		}
		fmt.Printf("]")

		iter.Close()
		snap.Close()
		store.Close()

		fmt.Printf("}")
	}
	fmt.Printf("]\n")

	return nil
}

func dumpKeyVal(key []byte, val []byte, toHex bool, firstDoc *bool) error {
	if toHex {
		if !*firstDoc {
			fmt.Printf(",")
		} else {
			*firstDoc = false
		}
		if val == nil {
			fmt.Printf("{\"k\":\"%s\"}", hex.EncodeToString(key))
		} else {
			fmt.Printf("{\"k\":\"%s\",\"v\":\"%s\"}",
				hex.EncodeToString(key), hex.EncodeToString(val))
		}
	} else {
		jBufk, err := json.Marshal(string(key))
		if err != nil {
			return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
		}
		if !*firstDoc {
			fmt.Printf(",")
		} else {
			*firstDoc = false
		}
		if val == nil {
			fmt.Printf("{\"k\":%s}", string(jBufk))
		} else {
			jBufv, err := json.Marshal(string(val))
			if err != nil {
				return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
			}
			fmt.Printf("{\"k\":%s,\"v\":%s}",
				string(jBufk), string(jBufv))
		}
	}
	return nil
}

func init() {
	RootCmd.AddCommand(dumpCmd)

	// Local flags that are intended to work with dump
	dumpCmd.Flags().BoolVar(&keysOnly, "keys-only", false,
		"Emits keys only")
	dumpCmd.Flags().StringVar(&keyPrefix, "key-prefix", "",
		"Emits only keys matching this key prefix. Example --key-prefix b")
	dumpCmd.Flags().BoolVar(&inHex, "hex", false,
		"Emits output in hex")
}
