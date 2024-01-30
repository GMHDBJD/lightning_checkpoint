package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
)

func replace(slice []int32, s int) []int32 {
	if s < len(slice) {
		slice[s] = -1
	}
	return slice
}

func main() {
	file := flag.String("file", "./tidb_lightning_checkpoint.pb", "checkpoint file")
	columnIdx := flag.Int("column", 5, "column index")
	dryRun := flag.Bool("dry-run", false, "dry run")

	flag.Parse()

	ctx := context.Background()
	cp, err := checkpoints.NewFileCheckpointsDB(ctx, *file)
	if err != nil {
		panic(errors.Trace(err))
	}
	for _, tb := range cp.Checkpoints.Checkpoints {
		for i, engine := range tb.Engines {
			for _, chunk := range engine.Chunks {
				fmt.Printf("engine%d, chunk:%v\n", i, chunk)
				if !*dryRun {
					chunk.ColumnPermutation = replace(chunk.ColumnPermutation, *columnIdx)
				}
			}
		}
	}

	err = cp.Close()
	if err != nil {
		panic(errors.Trace(err))
	}
}
