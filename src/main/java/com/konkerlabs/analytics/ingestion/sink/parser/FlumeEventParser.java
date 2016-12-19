package com.konkerlabs.analytics.ingestion.sink.parser;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flume.Event;
import org.joda.time.format.DateTimeFormatter;
import org.mortbay.log.Log;

import java.util.*;

public class FlumeEventParser {

    private final String timestampField;
    private final DateTimeFormatter dateTimeFormatter;
    private final Set<String> filter;

    public FlumeEventParser(String timestampField, DateTimeFormatter dateTimeFormatter, Set<String> filter) {
        this.timestampField = timestampField;
        this.dateTimeFormatter = dateTimeFormatter;
        this.filter = filter;
    }

    public List<Map<String, Object>> parse(List<Event> events) {
        List<Map<String, Object>> parsedEvents = new ArrayList<Map<String, Object>>();
        if (!CollectionUtils.isEmpty(events)) {
            for (Event event : events) {
                parsedEvents.add(parseEvent(event));
            }
        }
        return parsedEvents;
    }

    private Map<String, Object> parseEvent(Event event) {
        final Map<String, String> headers = event.getHeaders();
        Map<String, Object> parsedEvent = null;
        if (MapUtils.isNotEmpty(headers)) {
            parsedEvent = new HashMap<String, Object>();
            for (String header : headers.keySet()) {
                if (filter.contains(header)) {
                    if (timestampField.equals(header)) {
                        if (headers.containsValue(timestampField)) {
                            parsedEvent.put(timestampField, headers.get(timestampField));
                            //TODO create timestamp field when timestampField is null?
                            //parsedEvent.put(timestampField, new DateTime(DateTimeZone.UTC).toString(dateTimeFormatter));
                            //parsedEvent.put("timestamp", new DateTime(DateTimeZone.UTC).getMillis());
                        }
                    } else if (headers.containsValue(header)) {
                        parsedEvent.put(header, headers.get(header));
                    }
                }
            }
        }
        else {
            Log.warn("Headers are empty: " + event.getHeaders() +". Event will not be processed");
        }
        return parsedEvent;
    }

}