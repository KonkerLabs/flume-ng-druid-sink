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
                    for (JsonElement jsonArray : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new CountAggregatorFactory(jsonArray.getAsString()));
                    }
                    break;
                case DOUBLESUM:
                    for (JsonElement jsonArray : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new DoubleSumAggregatorFactory(jsonArray.getAsString() + "_sum", jsonArray.getAsString()));
                    }
                    break;
                case LONGSUM:
                    for (JsonElement jsonArray : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new LongSumAggregatorFactory(jsonArray.getAsString() + "_sum", jsonArray.getAsString()));
                    }
                    break;
                case DOUBLEMIN:
                    for (JsonElement jsonArray : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new DoubleMinAggregatorFactory(jsonArray.getAsString() + "_min", jsonArray.getAsString()));
                    }
                    break;
                case LONGMIN:
                    for (JsonElement jsonArray : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new LongMinAggregatorFactory(jsonArray.getAsString() + "_min", jsonArray.getAsString()));
                    }
                    break;
                case DOUBLEMAX:
                    for (JsonElement jsonArray : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new DoubleMaxAggregatorFactory(jsonArray.getAsString() + "_max", jsonArray.getAsString()));
                    }
                    break;
                case LONGMAX:
                    for (JsonElement jsonArray : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new LongMaxAggregatorFactory(jsonArray.getAsString() + "_max", jsonArray.getAsString()));
                    }
                    break;
                case HYPERUNIQUES:
                    for (JsonElement jsonArray : entry.getValue().getAsJsonArray()) {
                        aggregators.add(new HyperUniquesAggregatorFactory(jsonArray.getAsString() + "_unique", jsonArray.getAsString()));
                    }
                    break;
            }

        }

        return aggregators;

    }

}
