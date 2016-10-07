package com.konkerlabs.analytics.ingestion.sink.helper;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;

import java.util.ArrayList;
import java.util.List;

public class AggregatorsHelper {

    public static List<AggregatorFactory> build(String rawAggregators) {
        List<AggregatorFactory> aggregators = new ArrayList<AggregatorFactory>();
        aggregators.add(new CountAggregatorFactory("count"));
        return aggregators;

    }

}
