// Copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// footerStatsCmd represents the all command
var footerStatsCmd = &cobra.Command{
	Use:   "footer",
	Short: "Dumps aggregated stats from the latest footer of the store",
	Long: `This command dumps the aggregated stats from all segments
collected from the latest footer of the store.
	./mossScope stats footer <path_to_store>`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("at least one path is required")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeFooterStats(args)
	},
}

var getAll bool

func invokeFooterStats(dirs []string) error {
	if jsonFormat {
		fmt.Printf("[")
	}
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, readOnlyMode)
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}
		defer store.Close()

		currSnap, err := store.Snapshot()
		if err != nil || currSnap == nil {
			continue
		}

		type statsType map[string]interface{}
		footerStats := make(map[string]statsType)
		id := 1

		for {

			footer := currSnap.(*moss.Footer)
			footerID := fmt.Sprintf("Footer_%d", id)
			footerStats[footerID] = make(statsType)

			fetchFooterStats(footer, footerStats[footerID])

			if !getAll {
				break
			}

			prevSnap, err := store.SnapshotPrevious(currSnap)
			currSnap.Close()
			currSnap = prevSnap
			id++

			if err != nil || currSnap == nil {
				break
			}
		}

		if jsonFormat {
			jBuf, err := json.Marshal(footerStats)
			if err != nil {
				return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
			}
			if index != 0 {
				fmt.Printf(",")
			}
			fmt.Printf("{\"%s\":%s}", dir, string(jBuf))
		} else {
			fmt.Println(dir)
			for f, fstats := range footerStats {
				fmt.Printf("  %s\n", f)
				for k, v := range fstats {
					fmt.Printf("%25s : %v\n", k, v)
				}
			}
			fmt.Println()
		}
	}

	if jsonFormat {
		fmt.Printf("]\n")
	}

	return nil
}

func fetchFooterStats(footer *moss.Footer, stats map[string]interface{}) {
	if footer == nil {
		return
	}

	var totalOpsSet uint64
	var totalOpsDel uint64
	var totalKeyBytes uint64
	var totalValBytes uint64
	segmentBytes := make([]uint64, 0, len(footer.SegmentLocs))

	for i := range footer.SegmentLocs {
		sloc := &footer.SegmentLocs[i]

		totalOpsSet += sloc.TotOpsSet
		totalOpsDel += sloc.TotOpsDel
		totalKeyBytes += sloc.TotKeyByte
		totalValBytes += sloc.TotValByte
		segmentBytes = append(segmentBytes, sloc.TotKeyByte+sloc.TotValByte)
	}

	stats["segment_bytes"] = segmentBytes
	stats["num_segments"] = len(footer.SegmentLocs)
	stats["total_ops_set"] = totalOpsSet
	stats["total_ops_del"] = totalOpsDel
	stats["total_key_bytes"] = totalKeyBytes
	stats["total_val_bytes"] = totalValBytes
}

func init() {
	statsCmd.AddCommand(footerStatsCmd)

	// Local flags that are intended to work with stats footer
	footerStatsCmd.Flags().BoolVar(&getAll, "all", false,
		"Fetches stats from all available footers (Footer_1 is latest)")
	footerStatsCmd.Flags().BoolVar(&jsonFormat, "json", false,
		"Emits output in JSON")
}
