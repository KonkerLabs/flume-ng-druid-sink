package com.konkerlabs.analytics.ingestion.sink.helper;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.druid.query.aggregation.*;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class AggregatorsHelper {

    private enum AggregatorEnum {

        CARDINALITY,
        COUNT,
        FILTERED,
        HISTOGRAM,
        HYPERUNIQUES,
        JAVASCRIPT,
        DOUBLESUM,
        LONGSUM,
        DOUBLEMAX,
        LONGMAX,
        DOUBLEMIN,
        LONGMIN
    }

    public static List<AggregatorFactory> build(String rawAggregators) {
        List<AggregatorFactory> aggregators = new ArrayList<AggregatorFactory>();

        JsonElement jelement = new JsonParser().parse(rawAggregators);
        JsonObject jobject = jelement.getAsJsonObject();

        for (Map.Entry<String, JsonElement> entry : jobject.entrySet()) {

            switch (AggregatorEnum.valueOf(entry.getKey())) {
                case COUNT:
                    for (JsonElement element : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new CountAggregatorFactory(element.getAsString()));
                    }
                    break;
                case DOUBLESUM:
                    for (JsonElement element : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new DoubleSumAggregatorFactory(element.getAsString() + "_sum", element.getAsString()));
                    }
                    break;
                case LONGSUM:
                    for (JsonElement element : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new LongSumAggregatorFactory(element.getAsString() + "_sum", element.getAsString()));
                    }
                    break;
                case DOUBLEMIN:
                    for (JsonElement element : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new DoubleMinAggregatorFactory(element.getAsString() + "_min", element.getAsString()));
                    }
                    break;
                case LONGMIN:
                    for (JsonElement element : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new LongMinAggregatorFactory(element.getAsString() + "_min", element.getAsString()));
                    }
                    break;
                case DOUBLEMAX:
                    for (JsonElement element : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new DoubleMaxAggregatorFactory(element.getAsString() + "_max", element.getAsString()));
                    }
                    break;
                case LONGMAX:
                    for (JsonElement element : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new LongMaxAggregatorFactory(element.getAsString() + "_max", element.getAsString()));
                    }
                    break;
                case HYPERUNIQUES:
                    for (JsonElement element : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new HyperUniquesAggregatorFactory(element.getAsString() + "_unique", element.getAsString()));
                    }
                    break;
            }

        }

        return aggregators;

    }

}
