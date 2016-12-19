import com.konkerlabs.analytics.ingestion.sink.TranquilitySink;

import static junit.framework.Assert.assertTrue;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by Felipe on 18/11/16.
 */
public class TranquilitySinkTest
{
    private TranquilitySink _tranquilitySink ;
    private List<Event> _events;
    private Map<String, String> _parameters;

    @Before
    public void runBeforeTests()
    {
        boolean result = false;
        MemoryChannel _memoryChannel = new MemoryChannel();
        _memoryChannel.setName("memoryChannel-" + UUID.randomUUID());

        try
        {
            _tranquilitySink = new TranquilitySink();
            _parameters = new HashMap<String, String>();
            Properties properties = getProperties();

            if (!properties.isEmpty())
            {
                mapPropertiesToParameters(properties);
                Context context = new Context(_parameters);
                _tranquilitySink.configure(context);

                result = Configurables.configure(_memoryChannel, context);
                if (result)
                {
                    _tranquilitySink.setMockChannel(_memoryChannel);
                    _memoryChannel.start();
                    _tranquilitySink.start();
                }
            }
        }
        catch (Exception exception)
        {
            assertTrue(result);
        }

        _events = new ArrayList<Event>();
        fillEvents();

        assertTrue(result);
    }

    @Test
    public void processShouldBeReady()
    {
        try
        {
            Assert.assertEquals(_tranquilitySink.process(), Sink.Status.READY);
        }
        catch (EventDeliveryException e)
        {
            assertTrue(false);
        }
    }

    @Test
    public void eventsShouldBeSent()
    {
        Assert.assertTrue(_tranquilitySink.sendEvents());
    }

    public void fillEvents()
    {
        for (int i = 0; i < 100; i++)
        {
            final byte[] bytes = new byte[20];
            new Random().nextBytes(bytes);
            final Map<String, String> headers = new HashMap<String, String>();

            headers.put("date", Long.toString(System.currentTimeMillis()));
            headers.put("space", Arrays.toString(bytes));

            Event event = new Event()
            {
                Map<String, String> _headers = new HashMap<String, String>();
                byte[] _bytes = new byte[20];

                @Override
                public Map<String, String> getHeaders()
                {
                    return _headers;
                }

                @Override
                public void setHeaders(Map<String, String> map)
                {
                    _headers = map;
                }

                @Override
                public byte[] getBody()
                {
                    return _bytes;
                }

                @Override
                public void setBody(byte[] bytes)
                {
                    _bytes = bytes;
                }
            };

            event.setHeaders(headers);
            _events.add(event);
        }
        _tranquilitySink.setEvents(_events);
    }

    private Properties getProperties()
    {
        Properties properties = new Properties();
        InputStream input = null;
        try
        {
            input = this.getClass().getResourceAsStream("resources/test.conf");
            properties.load(input);
        }
        catch (IOException exception)
        {
            exception.printStackTrace();
        }
        finally
        {
            if (input != null)
            {
                try
                {
                    input.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        return properties;
    }

    private void mapPropertiesToParameters(Properties properties)
    {
        for (Object key : properties.keySet())
        {
            _parameters.put(key.toString(), properties.get(key).toString());
        }
    }
}
