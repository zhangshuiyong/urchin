
/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"d7y.io/dragonfly/v2/client/urchinfs"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var scheduleDescription = "schedule a urfs object to another center."

// scheduleCmd represents to schedule object between object storage and local.
// source urchinfs://源数据$endpoint/源数据$bucket/源数据filepath
var scheduleCmd = &cobra.Command{
	Use:                "schedule <source> <targetPeer> [flags]",
	Short:              scheduleDescription,
	Long:               scheduleDescription,
	Args:               cobra.ExactArgs(2),
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		urfs := urchinfs.New()
		peerResult, err := urfs.ScheduleDataToPeer(args[0], args[1])
		if err != nil{
			return err
		}
		fmt.Printf("schedule source to peer successful, info:%v\n", peerResult)
		return nil
	},
}

func init() {
	// Bind more cache specific persistent flags.
	flags := scheduleCmd.Flags()
	flags.String("filter", cfg.Filter, "filter is used to generate a unique task id by filtering unnecessary query params in the URL, it is separated by & character")
	flags.IntP("mode", "m", cfg.Mode, "mode is the mode in which the backend is written, when the value is 0, it represents AsyncWriteBack, and when the value is 1, it represents WriteBack")
	flags.Int("max-replicas", cfg.MaxReplicas, "maxReplicas is the maximum number of replicas of an object cache in seed peers")

	// Bind common flags.
	if err := viper.BindPFlags(flags); err != nil {
		panic(err)
	}
}
