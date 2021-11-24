// Copyright 2017-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Retrieves all the store related stats",
	Long: `This command can be used to dump stats available for the
specific moss store.
	./mossScope stats <sub-command> <path_to_store>`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("USAGE: mossScope stats <sub_command> <path_to_store>, " +
			"more details with --help")
	},
}

var jsonFormat bool

func init() {
	RootCmd.AddCommand(statsCmd)
}
