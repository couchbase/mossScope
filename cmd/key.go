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

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// keyCmd represents the key command
var keyCmd = &cobra.Command{
	Use:   "key",
	Short: "Dumps the key and value of the specified key",
	Long: `Dumps the key and value information of the requested key
from the latest snapshot in which it is available in JSON
format. For example:
	./mossScope dump key <keyname> <path_to_store> [flag]`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("a keyname along with at least one path " +
				"are required")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeKey(args[0], args[1:])
	},
}

var allVersions bool

func invokeKey(keyname string, dirs []string) error {
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

		currSnapshot := snap
		val, err := currSnapshot.Get([]byte(keyname), moss.ReadOptions{})
		if err == nil && val != nil {
			if index != 0 {
				fmt.Printf(",")
			}
			fmt.Printf("{\"%s\":[", dir)
			firstKey := true

			err = dumpKeyVal([]byte(keyname), val, inHex, &firstKey)
			if err != nil {
				return err
			}

			if allVersions {
				for {
					prevSnapshot, err := store.SnapshotPrevious(currSnapshot)
					currSnapshot.Close()
					currSnapshot = prevSnapshot

					if err != nil || currSnapshot == nil {
						break
					}

					val, err := currSnapshot.Get([]byte(keyname),
						moss.ReadOptions{})
					if err == nil && val != nil {
						err = dumpKeyVal([]byte(keyname), val, inHex, &firstKey)
						if err != nil {
							return err
						}
					}
				}
			}
			fmt.Printf("]}")
		}

		snap.Close()
		store.Close()

	}
	fmt.Printf("]\n")

	return nil
}

func init() {
	dumpCmd.AddCommand(keyCmd)

	// Local flags that are intended to work with dump key
	keyCmd.Flags().BoolVar(&allVersions, "all-versions", false,
		"Emits all the available versions of the key")
	keyCmd.Flags().BoolVar(&inHex, "hex", false,
		"Emits output in hex")
}
