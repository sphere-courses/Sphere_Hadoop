#!/usr/bin/env bash

# Run 20 iterations of PageRank computations
spark-submit --master yarn --deploy-mode cluster \
    --driver-memory 2G \
    --num-executors 10 \
    --executor-memory 2G \
    --class PageRankSpark ./build/libs/hw04-1.0-SNAPSHOT.jar 20 /data/hw4/soc-LiveJournal1.txt.gz ./PageRankSpark/Result
# Get result
hadoop fs -get ./PageRankSpark/Result