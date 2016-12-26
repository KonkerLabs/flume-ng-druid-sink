package com.konkerlabs.analytics.ingestion.sink;

import com.konkerlabs.analytics.ingestion.sink.helper.DateFormater;
import com.konkerlabs.analytics.ingestion.sink.helper.PropertiesReader;
import com.konkerlabs.analytics.ingestion.sink.parser.FlumeEventParser;
import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * Created by Felipe on 23/12/16.
 */
public class FlumeEventParserTest {
    private FlumeEventParser _flumeEventParser;
    private Properties _properties;

    @Before
    public void runBeforeTests() {
        _properties = PropertiesReader.getProperties();
        String timestampField = _properties.getProperty("timestampField");
        List<String> dimensions = Arrays.asList(_properties.getProperty("dimensions").split(","));
        Set<String> filter = new HashSet<String>();
        filter.add(timestampField);
        filter.addAll(dimensions);
        _flumeEventParser = new FlumeEventParser(timestampField,
                DateFormater.getDateFormatter(_properties.getProperty("timestampFormat")), filter);
    }

    @Test
    public void eventShouldBeParsed(){
        Assert.assertEquals(_flumeEventParser.parse(getEvents(true)).size(), 100);
    }

    @Test
    public void eventShouldNotBeParsed(){
        Assert.assertEquals(_flumeEventParser.parse(Collections.<Event>emptyList()).size(), 0);
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < 100; i++) {
            events.add(null);
        }
        Assert.assertEquals(_flumeEventParser.parse(events).size(), 0);
        Assert.assertEquals(_flumeEventParser.parse(getEvents(false)).size(), 0);
    }

    private List<Event> getEvents(boolean hasHeader){
        List<Event> events = new ArrayList<Event>();

        final byte[] bytes = new byte[20];
        new Random().nextBytes(bytes);
        final Map<String, String> headers = new HashMap<String, String>();
        headers.put("date", Long.toString(System.currentTimeMillis()));
        headers.put("space", Arrays.toString(bytes));

        for (int i = 0; i < 100; i++) {
            Event event = new Event() {
                Map<String, String> _headers = new HashMap<String, String>();
                byte[] _bytes = new byte[20];

                @Override
                public Map<String, String> getHeaders() {
                    return _headers;
                }

                @Override
                public void setHeaders(Map<String, String> map) {
                    _headers = map;
                }

                @Override
                public byte[] getBody() {
                    return _bytes;
                }

                @Override
                public void setBody(byte[] bytes) {
                    _bytes = bytes;
                }
            };
            if (hasHeader) {
                event.setHeaders(headers);
            }
            events.add(event);
        }

        return events;
    }

}
