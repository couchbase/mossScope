// Copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// footerCmd represents the footer command
var footerCmd = &cobra.Command{
	Use:   "footer",
	Short: "Dumps the latest footer in the store",
	Long: `This command will print out the latest footer in JSON
format, (optionally all) here's a sample command:
	./mossScope dump footer <path_to_store> [flag]`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("at least one path is required")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeFooter(args)
	},
}

var allAvailable bool

func invokeFooter(dirs []string) error {
	fmt.Printf("[")
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, readOnlyMode)
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}

		currSnap, err := store.Snapshot()
		if err != nil || currSnap == nil {
			return fmt.Errorf("Store-Snapshot() API failed, err: %v", err)
		}

		if index != 0 {
			fmt.Printf(",")
		}
		fmt.Printf("{\"%s\":[", dir)

		if allAvailable {
			for {
				jBuf, err := json.Marshal(currSnap.(*moss.Footer))
				if err != nil {
					return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
				}

				fmt.Printf("%s", string(jBuf))

				prevSnap, err := store.SnapshotPrevious(currSnap)
				currSnap.Close()
				currSnap = prevSnap

				if err != nil || currSnap == nil {
					fmt.Printf("")
					break
				}
				fmt.Printf(",")
			}
		} else {
			jBuf, err := json.Marshal(currSnap.(*moss.Footer))
			if err != nil {
				return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
			}

			fmt.Printf("%s", string(jBuf))

			currSnap.Close()
		}
		fmt.Printf("]}")

		store.Close()
	}
	fmt.Printf("]\n")

	return nil
}

func init() {
	dumpCmd.AddCommand(footerCmd)

	// Local flag that is intended to work with dump footer
	footerCmd.Flags().BoolVar(&allAvailable, "all", false,
		"Fetches all the available footers")
}
