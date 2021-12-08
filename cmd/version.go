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

// versionCmd represents the version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Retrieves the current version of mossScope",
	Run: func(cmd *cobra.Command, args []string) {
		emitVersion()
	},
}

func emitVersion() {
	fmt.Printf("mossScope v%s (moss lib version: %v)\n",
		version, moss.StoreVersion)
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
