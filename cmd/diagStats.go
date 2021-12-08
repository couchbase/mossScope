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
	"sort"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// diagStatsCmd represents the diag command
var diagStatsCmd = &cobra.Command{
	Use:   "diag",
	Short: "Dumps all the diagnostic stats",
	Long: `This command dumps all the diagnostic stats for the store.
	./mossScope stats diag <path_to_store>`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("at least one path is required")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeDiagStats(args)
	},
}

func invokeDiagStats(dirs []string) error {
	if jsonFormat {
		fmt.Printf("[")
	} else {
		emitVersion()
		fmt.Println()
	}

	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, readOnlyMode)
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}
		defer store.Close()

		snap, err := store.Snapshot()
		if err != nil || snap == nil {
			continue
		}

		footer := snap.(*moss.Footer)
		stats := make(map[string]interface{})

		fetchFooterStats(footer, stats)

		storeStats, err := store.Stats()
		if err != nil {
			return fmt.Errorf("Store-Stats() failed!, err: %v", err)
		}

		for k, v := range storeStats {
			stats[k] = v
		}

		if jsonFormat {
			jBuf, err := json.Marshal(stats)
			if err != nil {
				return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
			}
			if index != 0 {
				fmt.Printf(",")
			}
			fmt.Printf("{\"%s\":%s}", dir, string(jBuf))
		} else {
			fmt.Println(dir)
			var keys []string
			for k := range stats {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("%35s : %v\n", k, stats[k])
			}
			fmt.Println()
		}
	}

	if jsonFormat {
		fmt.Printf("]\n")
	}

	return nil
}

func init() {
	statsCmd.AddCommand(diagStatsCmd)

	// Local flag that is intended to work with stats diag
	diagStatsCmd.Flags().BoolVar(&jsonFormat, "json", false,
		"Emits output in JSON")
}
