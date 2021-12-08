// Copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cmd

import (
	"fmt"
	"strings"

	"github.com/couchbase/ghistogram"
	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// histCmd represents the hist command
var histCmd = &cobra.Command{
	Use:   "hist",
	Short: "Generates histograms for the store",
	Long: `This command generates histograms for various entities
available from the store.
	./mossScope stats hist <path_to_store>`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("at least one path is required")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeHistStats(args)
	},
}

func invokeHistStats(dirs []string) error {
	for _, dir := range dirs {
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
			return fmt.Errorf("Snaphot-StartItr() API failed, err: %v", err)
		}

		keySizes := ghistogram.NewNamedHistogram("KeySizes(B) ", 10, 4, 4)
		valSizes := ghistogram.NewNamedHistogram("ValSizes(B) ", 10, 4, 4)

		for {
			k, v, err := iter.Current()
			if err != nil {
				break
			}

			if len(keyPrefix) != 0 {
				// A specific keyPrefix has been requested
				if strings.HasPrefix(string(k), keyPrefix) {
					keySizes.Add(uint64(len(k)), 1)
					valSizes.Add(uint64(len(v)), 1)
				}
			} else {
				keySizes.Add(uint64(len(k)), 1)
				valSizes.Add(uint64(len(v)), 1)
			}

			if iter.Next() == moss.ErrIteratorDone {
				break
			}
		}

		fmt.Printf("\"%s\"\n", dir)
		fmt.Println((keySizes.EmitGraph(nil, nil)).String())
		fmt.Println((valSizes.EmitGraph(nil, nil)).String())

		iter.Close()
		snap.Close()
		store.Close()
	}

	return nil
}

func init() {
	statsCmd.AddCommand(histCmd)

	// Local flag that is intended to work with stats hist
	histCmd.Flags().StringVar(&keyPrefix, "key-prefix", "",
		"Emits histograms of keys that begin with the specified prefix")
}
