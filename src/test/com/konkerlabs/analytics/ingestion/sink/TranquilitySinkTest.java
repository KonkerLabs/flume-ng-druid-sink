package com.konkerlabs.analytics.ingestion.sink;

import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.*;

import com.konkerlabs.analytics.ingestion.sink.helper.DateFormater;
import com.konkerlabs.analytics.ingestion.sink.helper.PropertiesReader;
import com.konkerlabs.analytics.ingestion.sink.parser.FlumeEventParser;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.apache.flume.*;
import org.apache.flume.channel.AbstractChannel;
import org.apache.flume.conf.Configurables;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.runtime.BoxedUnit;

import java.util.*;

/**
 * Created by Felipe on 22/12/16.
 */
public class TranquilitySinkTest {
    private TranquilitySink _tranquilitySink;
    private Map<String, String> _parameters;
    private AbstractChannel _channel;
    private Properties _properties;
    private FlumeEventParser _flumeEventParser;
    private Tranquilizer _mockDruidService;


    private class TestSinkStrategy implements TranquilitySink.SinkStrategy {
        @Override
        public Tranquilizer getDruidService() {
            return _mockDruidService;
        }
    }

    @Before
    public void runBeforeTests() {
        boolean result = false;
        _channel = Mockito.mock(AbstractChannel.class);
        _channel.setName("memoryChannel-" + UUID.randomUUID());
        _mockDruidService = Mockito.mock(Tranquilizer.class);
        try {
            _tranquilitySink = new TranquilitySink();

            TestSinkStrategy testSinkStrategy = new TestSinkStrategy();
            _tranquilitySink.setSinkStrategy(testSinkStrategy);
            _parameters = new HashMap<String, String>();
            _properties = PropertiesReader.getProperties();

            if (!_properties.isEmpty()) {
                mapPropertiesToParameters(_properties);
                Context context = new Context(_parameters);

                _tranquilitySink.configure(context);

                result = Configurables.configure(_channel, context);
                if (result) {
                    _tranquilitySink.setChannel(_channel);
                    _tranquilitySink.start();
                }
            }
        } catch (Exception exception) {
            assertTrue(result);
        }

        assertTrue(result);
        setFlumeEventParser();
    }

    @Test
    public void dateFieldShouldBeProcessed() {
        final byte[] bytes = new byte[20];
        new Random().nextBytes(bytes);

        final Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("date", Long.toString(System.currentTimeMillis()));
        headers.put("space", Arrays.toString(bytes));

        Timestamper<Map<String, Object>> timestamper = _tranquilitySink.getTimestamper();
        DateTime dateTime = timestamper.timestamp(headers);
        DateTime now = DateTime.now();
        Assert.assertTrue(dateTime.getYear() == now.getYear() && dateTime.getMonthOfYear() == now.getMonthOfYear()
                && dateTime.getDayOfMonth() == now.getDayOfMonth());

        headers.clear();
        headers.put("date", DateTime.now().toString());
        headers.put("space", Arrays.toString(bytes));

        Assert.assertTrue(dateTime.getYear() == now.getYear() && dateTime.getMonthOfYear() == now.getMonthOfYear()
                && dateTime.getDayOfMonth() == now.getDayOfMonth());
    }

    @Test(expected = NullPointerException.class)
    public void dateFieldShouldNotBeProcessed() {
        final byte[] bytes = new byte[20];
        new Random().nextBytes(bytes);

        final Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("space", Arrays.toString(bytes));

        Timestamper<Map<String, Object>> timestamper = _tranquilitySink.getTimestamper();
        // Null Pointer Exception expected
        DateTime dateTime = timestamper.timestamp(headers);
        DateTime now = DateTime.now();
        Assert.assertTrue(dateTime.getYear() == now.getYear() && dateTime.getMonthOfYear() == now.getMonthOfYear()
                && dateTime.getDayOfMonth() == now.getDayOfMonth());

        dateTime = timestamper.timestamp(null);
        Assert.assertNull(dateTime);
    }

    @Test
    public void eventShouldBeCreated(){
        Event event = getEvent();
        when(_channel.take()).thenReturn(event);
        Assert.assertNotNull(_tranquilitySink.buildEvent(_channel));
    }

    @Test
    public void eventShouldBeCaught(){
        Event event = getEvent();
        when(_channel.take()).thenReturn(event);
        Assert.assertEquals(_tranquilitySink.takeEventsFromChannel(_channel, 100).size(), 100);
    }

    @Test
    public void processShouldBeBackoff() {
        try {
            Event event = getEvent();
            Transaction transaction = Mockito.mock(Transaction.class);
            InOrder inOrder = Mockito.inOrder(transaction);

            when(_channel.take()).thenReturn(event);
            when(_channel.getTransaction()).thenReturn(transaction);
            doThrow(new ChannelException("")).when(transaction).commit();

            mockFuture();

            Assert.assertEquals(_tranquilitySink.process(), Sink.Status.BACKOFF);
            inOrder.verify(transaction, times(1)).begin();
            inOrder.verify(transaction, times(1)).rollback();

            mockSendEventsOnFailure(new Exception());
            Assert.assertEquals(_tranquilitySink.process(), Sink.Status.BACKOFF);
            inOrder.verify(transaction, times(1)).begin();
            inOrder.verify(transaction, times(1)).rollback();

            mockSendEventsOnFailure(new EventDeliveryException());
            Assert.assertEquals(_tranquilitySink.process(), Sink.Status.BACKOFF);
            inOrder.verify(transaction, times(1)).begin();
            inOrder.verify(transaction, times(1)).rollback();
        } catch (EventDeliveryException e) {
            assertTrue(false);
        }
    }

    @Test
    public void processShouldBeReady() {
        try {
            Event event = getEvent();
            Transaction transaction = Mockito.mock(Transaction.class);
            InOrder inOrder = Mockito.inOrder(transaction);
            when(_channel.take()).thenReturn(event);
            when(_channel.getTransaction()).thenReturn(transaction);

            mockFuture();

            Assert.assertEquals(_tranquilitySink.process(), Sink.Status.READY);
            inOrder.verify(transaction, times(1)).begin();
            inOrder.verify(transaction, times(1)).close();
        } catch (EventDeliveryException e) {
            assertTrue(false);
        }
    }

    @Test
    public void eventsShouldBeSent() {
        List<Event> events = getEvents();
        mockSendEventsOnSuccess(BoxedUnit.UNIT);
        Assert.assertEquals(_tranquilitySink.sendEvents(_flumeEventParser.parse(events)), 100);
    }

    @Test
    public void eventsShouldNotBeProcessed() {
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < 100; i++) {
            events.add(null);
        }

        mockFuture();

        Assert.assertEquals(_tranquilitySink.sendEvents(_flumeEventParser.parse(events)), 0);
        Assert.assertEquals(_tranquilitySink.sendEvents(_flumeEventParser.parse(Collections.<Event>emptyList())), 0);
    }

    @Test
    public void eventsShouldBeDropped() {
        List<Event> events = new ArrayList<Event>();
        mockSendEventsOnFailure(MessageDroppedException.Instance());
        Assert.assertEquals(_tranquilitySink.sendEvents(_flumeEventParser.parse(events)), 0);
    }

    @Test
    public void eventsShouldNotBeSent() {
        List<Event> events = getEvents();
        mockSendEventsOnFailure(new Exception());
        Assert.assertEquals(_tranquilitySink.sendEvents(_flumeEventParser.parse(events)), 100);
    }

    private void mockFuture() {
        Future future = Mockito.mock(Future.class);
        when(_mockDruidService.send(Matchers.<Map<String, Object>>anyObject())).thenReturn(future);
    }

    private void mockSendEventsOnFailure(final Throwable throwable) {
        Future future = Mockito.mock(Future.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((FutureEventListener) invocation.getArguments()[0]).onFailure(throwable);
                return null;
            }
        }).when(future).addEventListener(Matchers.<FutureEventListener>any());
        when(_mockDruidService.send(Matchers.<Map<String, Object>>anyObject()))
                .thenReturn(future);
    }

    private void mockSendEventsOnSuccess(final BoxedUnit boxedUnit) {
        Future future = Mockito.mock(Future.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((FutureEventListener) invocation.getArguments()[0]).onSuccess(boxedUnit);
                return null;
            }
        }).when(future).addEventListener(Matchers.<FutureEventListener>any());
        when(_mockDruidService.send(Matchers.<Map<String, Object>>anyObject())).thenReturn(future);
    }

    private Event getEvent() {
        final byte[] bytes = new byte[20];
        new Random().nextBytes(bytes);
        final Map<String, String> headers = new HashMap<String, String>();

        headers.put("date", Long.toString(System.currentTimeMillis()));
        headers.put("space", Arrays.toString(bytes));

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

        event.setHeaders(headers);
        return event;
    }

    private List<Event> getEvents() {
        List<Event> events = new ArrayList<Event>();
        for (int i = 0; i < 100; i++) {
            events.add(getEvent());
        }
        return events;
    }

    private void mapPropertiesToParameters(Properties properties) {
        for (Object key : properties.keySet()) {
            _parameters.put(key.toString(), properties.get(key).toString());
        }
    }

    private void setFlumeEventParser() {
        String timestampField = _properties.getProperty("timestampField");
        List<String> dimensions = Arrays.asList(_properties.getProperty("dimensions").split(","));
        Set<String> filter = new HashSet<String>();
        filter.add(timestampField);
        filter.addAll(dimensions);
        _flumeEventParser = new FlumeEventParser(timestampField,
                DateFormater.getDateFormatter(_properties.getProperty("timestampFormat")), filter);
    }
}
