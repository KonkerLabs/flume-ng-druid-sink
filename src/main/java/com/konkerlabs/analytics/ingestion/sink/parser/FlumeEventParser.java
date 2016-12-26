package com.konkerlabs.analytics.ingestion.sink.parser;

import com.konkerlabs.analytics.ingestion.sink.exception.FlumeEventParserException;
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
        for (Event event : events) {
            try {
                Map<String, Object> parsedEvent = parseEvent(event);
                parsedEvents.add(parsedEvent);
            } catch (FlumeEventParserException e) {
                Log.warn("Event is null or headers are empty");
                return Collections.emptyList();
            }
        }
        return parsedEvents;
    }

    private Map<String, Object> parseEvent(Event event) throws FlumeEventParserException {
        Map<String, Object> parsedEvent;
        if (event != null && !event.getHeaders().isEmpty()) {
            final Map<String, String> headers = event.getHeaders();
            Log.debug("Headers: {}", headers);
            parsedEvent = new HashMap<String, Object>();
            Log.debug("Header keys: {}", headers.keySet());
            for (String header : headers.keySet()) {
                if (filter.contains(header)) {
                    if (headers.get(timestampField) != null && timestampField.equals(header)) {
                        parsedEvent.put(timestampField, headers.get(timestampField));
                        //TODO create timestamp field when timestampField is null?
                        //parsedEvent.put(timestampField, new DateTime(DateTimeZone.UTC).toString(dateTimeFormatter));
                        //parsedEvent.put("timestamp", new DateTime(DateTimeZone.UTC).getMillis());
                    } else if (headers.get(header) != null) {
                        parsedEvent.put(header, headers.get(header));
                    }
                }
            }
        } else {
            throw new FlumeEventParserException("Event is null or headers are empty");
        }

        return parsedEvent;
    }

}