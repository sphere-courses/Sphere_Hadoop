#!/usr/bin/env bash

# Prepare data for MR task
hadoop jar ./build/libs/hw04-1.0-SNAPSHOT.jar PageRankMRInit /data/hw4/soc-LiveJournal1.txt.gz ./PageRankMR/Iters
# Run 20 iterations of PageRank computations
hadoop jar ./build/libs/hw04-1.0-SNAPSHOT.jar PageRankMR 20 ./PageRankMR/Iters
# Sort all pages with respect to evaluated PageRank
hadoop jar ./build/libs/hw04-1.0-SNAPSHOT.jar PageRankMRеGetTop  ./PageRankMR/Iters/20/part-r-* ./PageRankMR/Result ./PageRankMR/Iters/leakedPR/20
# Get result
hadoop fs -get ./PageRankMR/Result