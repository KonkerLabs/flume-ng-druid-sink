package com.konkerlabs.analytics.ingestion.sink.parser;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flume.Event;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlumeEventParser {

    private final String timestampField;
    private final DateTimeFormatter dateTimeFormatter;
    private final List<String> dimensions;

    public FlumeEventParser(String timestampField, DateTimeFormatter dateTimeFormatter, List<String> dimensions) {
        this.timestampField = timestampField;
        this.dateTimeFormatter = dateTimeFormatter;
        this.dimensions = dimensions;
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
                if (dimensions.contains(header)) {
                    if (timestampField.equalsIgnoreCase(header)) {
//                    parsedEvent.put(timestampField, dateTimeFormatter.parseDateTime(headers.get(timestampField)));
                        //TODO create timestamp field when timestampField is null.
                        parsedEvent.put(timestampField, new DateTime(DateTimeZone.UTC).toString(dateTimeFormatter));
//                    parsedEvent.put("timestamp", new DateTime(DateTimeZone.UTC).getMillis());
                    } else
                        parsedEvent.put(header, headers.get(header));
                }
            }
        }
        return parsedEvent;
    }

}