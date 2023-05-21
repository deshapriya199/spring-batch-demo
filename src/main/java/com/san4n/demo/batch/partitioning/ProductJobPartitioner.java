package com.san4n.demo.batch.partitioning;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class ProductJobPartitioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitionData = new HashMap<>();

        long val = 1000;

        for (int i = 0; i < gridSize; i++) {
            ExecutionContext executionContext = new ExecutionContext();

            // give fileName for ExecutionContext
            executionContext.putLong("from", ((val * i)+1));
            // give a thread name for ExecutionContext
            executionContext.putLong("to", (val * (i+1)));

            executionContext.putString("pName", String.valueOf(i));

            partitionData.put("partition: " + i, executionContext);
        }

        return partitionData;
    }
}
