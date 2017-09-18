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
	"fmt"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// compactCmd represents the compact command
var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "Compacts an offline moss store",
	Long: `Compacts a moss store.  Must ONLY be invoked when all other
processes using the moss store have completely stopped running.
WARNING: Running this command with concurrent data mutations can result
in data loss.
For example:
	./mossScope compact <path_to_store>`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("at least one path is required")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeCompact(args)
	},
}

func invokeCompact(dirs []string) error {
	fmt.Printf("[")
	for _, dir := range dirs {
		store, coll, err := moss.OpenStoreCollection(dir,
			moss.StoreOptions{}, moss.StorePersistOptions{})
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStoreCollection() API failed, err: %v",
				err)
		}
		// A snapshot of a freshly opened collection should be empty.
		emptySnap, err := coll.Snapshot()
		if err != nil {
			return fmt.Errorf("Moss-Snapshot failed, err: %v", err)
		}

		// Attempting to persist an empty snapshot should trigger compaction.
		storePersistOpts := moss.StorePersistOptions{
			CompactionConcern: moss.CompactionAllow,
		}
		snap, err := store.Persist(emptySnap, storePersistOpts)
		if err != nil || snap == nil {
			return fmt.Errorf("Store-Persist() API failed, err: %v", err)
		}

		fmt.Printf("{ \"%s\" : \"compaction done.\" }\n", dir)

		snap.Close()
		coll.Close()
		store.Close()
	}
	fmt.Printf("]\n")

	return nil
}

func init() {
	RootCmd.AddCommand(compactCmd)
}
