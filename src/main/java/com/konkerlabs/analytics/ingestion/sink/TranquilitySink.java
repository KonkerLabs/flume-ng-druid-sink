package com.konkerlabs.analytics.ingestion.sink;

/**
 * Created by renatoochando on 07/10/16.
 */

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.konkerlabs.analytics.ingestion.sink.helper.AggregatorsHelper;
import com.konkerlabs.analytics.ingestion.sink.parser.FlumeEventParser;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;
import com.twitter.util.FutureEventListener;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.util.*;

public class TranquilitySink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(TranquilitySink.class);

    private static final String INDEX_SERVICE = "indexService";
    private static final String FIREHOSE_PATTERN = "firehosePattern";
    private static final String DISCOVERY_PATH = "discoveryPath";
    private static final String DATA_SOURCE = "dataSource";
    private static final String DIMENSIONS = "dimensions";
    private static final String AGGREGATORS = "aggregators";
    private static final String ZOOKEEPER_LOCATION = "zookeeperLocation";
    private static final String TIMESTAMP_FIELD = "timestampField";
    private static final String TIMESTAMP_FORMAT = "timestampFormat";
    private static final String SEGMENT_GRANULARITY = "segmentGranularity";
    private static final String BATCH_SIZE = "batchSize";
    private static final String QUERY_GRANULARITY = "queryGranularity";
    private static final String WINDOW_PERIOD = "windowPeriod";
    private static final String PARTITIONS = "partitions";
    private static final String REPLICANTS = "replicants";
    private static final String ZOOKEEPPER_BASE_SLEEP_TIME = "baseSleepTime";
    private static final String ZOOKEEPER_MAX_RETRIES = "maxRetries";
    private static final String ZOOKEEPER_MAX_SLEEP = "maxSleep";
    private static final String DEFAULT_FIREHOSE = "firehose:druid:%s";
    private static final String DEFAUL_DATASOURCE = "fooDataSource";
    private static final String DEFAULT_QUERY_GRANULARITY = "NONE";
    private static final String DEFAULT_SEGMENT_GRANULARITY = "HOUR";
    private static final String DEFAULT_PERIOD = "PT10M";
    private static final Integer DEFAULT_PARTITIONS = 1;
    private static final Integer DEFAULT_REPLICANTS = 1;
    private static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";
    private static final String DEFAULT_ZOOKEEPER_LOCATION = "localhost:2181";
    private static final Integer DEFAULT_ZOOKEEPER_BASE_SLEEP = 1000;
    private static final Integer DEFAULT_ZOOKEEPER_MAX_RETRIES = 3;
    private static final Integer DEFAULT_ZOOKEEPER_MAX_SLEEP = 30000;
    private static final Integer DEFAULT_BATCH_SIZE = 10000;

    private Tranquilizer<Map<String, Object>> druidService;
    private CuratorFramework curatorFramework;
    private String discoveryPath;
    private String indexService;
    private String firehosePattern;
    private String dataSource;
    private List<String> dimensions;
    private List<AggregatorFactory> aggregators;
    private SinkCounter sinkCounter;
    private Integer batchSize;
    private QueryGranularity queryGranularity;
    private Granularity segmentGranularity;
    private String windowPeriod;
    private int partitions;
    private int replicants;
    private String zookeeperLocation;
    private int baseSleppTime;
    private int maxRetries;
    private int maxSleep;
    private String timestampField;
    private String timestampFormat;
    private DateTimeFormatter dateTimeFormatter;
    private FlumeEventParser eventParser;

    @Override
    public void configure(Context context) {
        indexService = context.getString(INDEX_SERVICE);
        discoveryPath = context.getString(DISCOVERY_PATH);
        dimensions = Arrays.asList(context.getString(DIMENSIONS).split(","));
        firehosePattern = context.getString(FIREHOSE_PATTERN, DEFAULT_FIREHOSE);
        dataSource = context.getString(DATA_SOURCE, DEFAUL_DATASOURCE);
        aggregators = AggregatorsHelper.build(context.getString(AGGREGATORS));
        queryGranularity = QueryGranularity.fromString(context.getString(QUERY_GRANULARITY, DEFAULT_QUERY_GRANULARITY));
        segmentGranularity = Granularity.valueOf(context.getString(SEGMENT_GRANULARITY, DEFAULT_SEGMENT_GRANULARITY));
        windowPeriod = context.getString(WINDOW_PERIOD, DEFAULT_PERIOD);
        partitions = context.getInteger(PARTITIONS, DEFAULT_PARTITIONS);
        replicants = context.getInteger(REPLICANTS, DEFAULT_REPLICANTS);
        timestampField = context.getString(TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
        timestampFormat = context.getString(TIMESTAMP_FORMAT, null);
        zookeeperLocation = context.getString(ZOOKEEPER_LOCATION, DEFAULT_ZOOKEEPER_LOCATION);
        baseSleppTime = context.getInteger(ZOOKEEPPER_BASE_SLEEP_TIME, DEFAULT_ZOOKEEPER_BASE_SLEEP);
        maxRetries = context.getInteger(ZOOKEEPER_MAX_RETRIES, DEFAULT_ZOOKEEPER_MAX_RETRIES);
        maxSleep = context.getInteger(ZOOKEEPER_MAX_SLEEP, DEFAULT_ZOOKEEPER_MAX_SLEEP);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

        druidService = buildDruidService();
        sinkCounter = new SinkCounter(this.getName());
        if (timestampFormat.equals("auto"))
            dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        else if (timestampFormat.equals("millis"))
            dateTimeFormatter = null;
        else
            dateTimeFormatter = DateTimeFormat.forPattern(timestampFormat);

        // Filter defined fields
        Set<String> filter = new HashSet<String>();
        filter.add(timestampField);
        filter.addAll(dimensions);
        for (AggregatorFactory aggregatorFactory : aggregators) {
            filter.addAll(aggregatorFactory.requiredFields());
        }

        eventParser = new FlumeEventParser(timestampField, dateTimeFormatter, filter);
    }

    private Tranquilizer<Map<String, Object>> buildDruidService() {
        curatorFramework = buildCuratorFramework();
        final TimestampSpec timestampSpec = new TimestampSpec(timestampField, timestampFormat, null);
        final Timestamper<Map<String, Object>> timestamper = getTimestamper();
        final DruidLocation druidLocation = DruidLocation.create(indexService, firehosePattern, dataSource);
        final DruidRollup druidRollup = DruidRollup
                .create(DruidDimensions.specific(dimensions), aggregators, queryGranularity);
        final ClusteredBeamTuning clusteredBeamTuning = ClusteredBeamTuning.builder()
                .segmentGranularity(segmentGranularity)
                .windowPeriod(new Period(windowPeriod)).partitions(partitions).replicants(replicants).build();

        return DruidBeams.builder(timestamper).curator(curatorFramework).discoveryPath(discoveryPath).location(
                druidLocation).timestampSpec(timestampSpec).rollup(druidRollup).tuning(clusteredBeamTuning)
                .buildTranquilizer();
    }

    @Override
    public Status process() throws EventDeliveryException {
        List<Event> events;
        List<Map<String, Object>> parsedEvents;
        Status status = Status.BACKOFF;
        Transaction transaction = this.getChannel().getTransaction();
        try {
            transaction.begin();
            events = takeEventsFromChannel(this.getChannel(), batchSize);
            status = Status.READY;
            if (!events.isEmpty()) {
                updateSinkCounters(events);
                parsedEvents = eventParser.parse(events);
                sendEvents(parsedEvents);
                sinkCounter.addToEventDrainSuccessCount(events.size());
            } else {
                sinkCounter.incrementBatchEmptyCount();
            }
            transaction.commit();
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
            this.sinkCounter.incrementConnectionFailedCount();
        } catch (Throwable t) {
            t.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                LOG.error(t.getMessage());
                throw new EventDeliveryException("An error occurred sending events to druid", t);
            }
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public synchronized void start() {
        this.sinkCounter.start();
        this.druidService.start();

        super.start();
    }

    private void updateSinkCounters(List<Event> events) {
        if (events.size() == batchSize) {
            sinkCounter.incrementBatchCompleteCount();
        } else {
            sinkCounter.incrementBatchUnderflowCount();
        }
    }

    private List<Event> takeEventsFromChannel(Channel channel, long eventsToTake) throws ChannelException {
        List<Event> events = new ArrayList<Event>();
        Event event;
        for (int i = 0; i < eventsToTake; i++) {
            event = buildEvent(channel);
            events.add(event);
            if (event != null) {
                sinkCounter.incrementEventDrainAttemptCount();
            }
        }
        events.removeAll(Collections.singleton(null));
        return events;
    }

    private Event buildEvent(Channel channel) {
        final Event takenEvent = channel.take();
        final ObjectNode objectNode = new ObjectNode(JsonNodeFactory.instance);
        Event event = null;
        if (takenEvent != null) {
            event = EventBuilder.withBody(objectNode.toString().getBytes(Charsets.UTF_8),
                    takenEvent.getHeaders());
        }
        return event;
    }

    private int sendEvents(List<Map<String, Object>> events) {
        int sentEvents = 0;

        LOG.debug("Sending events.");
        for (final Map<String, Object> item : events) {
            druidService.send(item).addEventListener(
                    new FutureEventListener<BoxedUnit>() {
                        @Override
                        public void onSuccess(BoxedUnit value) {
                            LOG.debug("Sent message: " + Arrays.toString(item.entrySet().toArray()));
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            if (e instanceof MessageDroppedException) {
                                LOG.warn("Dropped message: " + Arrays.toString(item.entrySet().toArray()), e);
                            } else {
                                LOG.error("Failed to send message: " + Arrays.toString(item.entrySet().toArray()), e);
                            }
                        }
                    }
            );
            sentEvents++;
        }
        LOG.info("Total sent messages: " + sentEvents);

        return sentEvents;
    }

    private CuratorFramework buildCuratorFramework() {
        final CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString(zookeeperLocation)
                .retryPolicy(new ExponentialBackoffRetry(baseSleppTime, maxRetries, maxSleep))
                .build();
        curator.start();

        return curator;
    }

    private Timestamper<Map<String, Object>> getTimestamper() {
        return new Timestamper<Map<String, Object>>() {
            @Override
            public DateTime timestamp(Map<String, Object> theMap) {
                if (timestampFormat.equals("millis"))
                    return new DateTime(Long.valueOf((String) theMap.get(timestampField)));
                else
                    return dateTimeFormatter.parseDateTime((String) theMap.get(timestampField));
            }
        };
    }
}
