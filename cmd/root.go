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
	"os"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "mossScope",
	Short: "A diagnostic tool for moss-store directories",
	Long: `This CLI tool is designed to assist in diagnosing moss-store
directories. It comprises of a dump facility with options, stats
facility with options, an option to compact the specified moss
store, among other things.`,
}

var version = "0.1.0"
var keyPrefix string

var readOnlyMode = moss.StoreOptions{KeepFiles: true,
	CollectionOptions: moss.CollectionOptions{ReadOnly: true}}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
