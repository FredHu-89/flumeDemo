package office;

/**
 * Created by Hu Feng on 2017/7/21.
 */

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSink
        extends AbstractSink
        implements Configurable
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private final Properties kafkaProps = new Properties();
    private KafkaProducer<String, byte[]> producer;
    private String topic;
    private int batchSize;
    private List<Future<RecordMetadata>> kafkaFutures;
    private KafkaSinkCounter counter;
    private boolean useAvroEventFormat;
    private String partitionHeader = null;
    private Integer staticPartitionId = null;
    private Optional<SpecificDatumWriter> writer = Optional.absent();
    private Optional<SpecificDatumReader<AvroFlumeEvent>> reader = Optional.absent();
    private Optional<ByteArrayOutputStream> tempOutStream = Optional.absent();
    private BinaryEncoder encoder = null;

    public String getTopic()
    {
        return this.topic;
    }

    public int getBatchSize()
    {
        return this.batchSize;
    }

    public Sink.Status process()
            throws EventDeliveryException
    {
        Sink.Status result = Sink.Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;
        try
        {
            long processedEvents = 0L;

            transaction = channel.getTransaction();
            transaction.begin();

            this.kafkaFutures.clear();
            long batchStartTime = System.nanoTime();
            for (; processedEvents < this.batchSize; processedEvents += 1L)
            {
                event = channel.take();
                if (event == null)
                {
                    if (processedEvents == 0L)
                    {
                        result = Sink.Status.BACKOFF;
                        this.counter.incrementBatchEmptyCount(); break;
                    }
                    this.counter.incrementBatchUnderflowCount();

                    break;
                }
                byte[] eventBody = event.getBody();
                Map<String, String> headers = event.getHeaders();

                eventTopic = (String)headers.get("topic");
                if (eventTopic == null) {
                    eventTopic = this.topic;
                }
                eventKey = (String)headers.get("key");
                if (logger.isTraceEnabled()) {
                    if (LogPrivacyUtil.allowLogRawData()) {
                        logger.trace("{Event} " + eventTopic + " : " + eventKey + " : " + new String(eventBody, "UTF-8"));
                    } else {
                        logger.trace("{Event} " + eventTopic + " : " + eventKey);
                    }
                }
                logger.debug("event #{}", Long.valueOf(processedEvents));


                long startTime = System.currentTimeMillis();

                Integer partitionId = null;
                try
                {
                    if (this.staticPartitionId != null) {
                        partitionId = this.staticPartitionId;
                    }
                    if (this.partitionHeader != null)
                    {
                        String headerVal = (String)event.getHeaders().get(this.partitionHeader);
                        if (headerVal != null) {
                            partitionId = Integer.valueOf(Integer.parseInt(headerVal));
                        }
                    }
//                    ProducerRecord<String, byte[]> record;
                    ProducerRecord<String, byte[]> record;
                    if (partitionId != null) {
                        record = new ProducerRecord(eventTopic, partitionId, eventKey, serializeEvent(event, this.useAvroEventFormat));
                    } else {
                        record = new ProducerRecord(eventTopic, eventKey, serializeEvent(event, this.useAvroEventFormat));
                    }
                    this.kafkaFutures.add(this.producer.send(record, new SinkCallback(startTime)));
                }
                catch (NumberFormatException ex)
                {
                    throw new EventDeliveryException("Non integer partition id specified", ex);
                }
                catch (Exception ex)
                {
                    throw new EventDeliveryException("Could not send event", ex);
                }
            }
            this.producer.flush();
            if (processedEvents > 0L)
            {
                for (Future<RecordMetadata> future : this.kafkaFutures) {
                    future.get();
                }
                long endTime = System.nanoTime();
                this.counter.addToKafkaEventSendTimer((endTime - batchStartTime) / 1000000L);
                this.counter.addToEventDrainSuccessCount(Long.valueOf(this.kafkaFutures.size()).longValue());
            }
            transaction.commit();
        }
        catch (Exception ex)
        {
            String errorMsg = "Failed to publish events";
            logger.error("Failed to publish events", ex);
            result = Sink.Status.BACKOFF;
            if (transaction != null) {
                try
                {
                    this.kafkaFutures.clear();
                    transaction.rollback();
                    this.counter.incrementRollbackCount();
                }
                catch (Exception e)
                {
                    logger.error("Transaction rollback failed", e);
                    throw Throwables.propagate(e);
                }
            }
            throw new EventDeliveryException(errorMsg, ex);
        }
        finally
        {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
    }

    public synchronized void start()
    {
        this.producer = new KafkaProducer(this.kafkaProps);
        this.counter.start();
        super.start();
    }

    public synchronized void stop()
    {
        this.producer.close();
        this.counter.stop();
        logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), this.counter);
        super.stop();
    }

    public void configure(Context context)
    {
        translateOldProps(context);

        String topicStr = context.getString("kafka.topic");
        if ((topicStr == null) || (topicStr.isEmpty()))
        {
            topicStr = "default-flume-topic";
            logger.warn("Topic was not specified. Using {} as the topic.", topicStr);
        }
        else
        {
            logger.info("Using the static topic {}. This may be overridden by event headers", topicStr);
        }
        this.topic = topicStr;

        this.batchSize = context.getInteger("flumeBatchSize", Integer.valueOf(100)).intValue();
        if (logger.isDebugEnabled()) {
            logger.debug("Using batch size: {}", Integer.valueOf(this.batchSize));
        }
        this.useAvroEventFormat = context.getBoolean("useFlumeEventFormat", Boolean.valueOf(false)).booleanValue();


        this.partitionHeader = context.getString("partitionIdHeader");
        this.staticPartitionId = context.getInteger("defaultPartitionId");
        if (logger.isDebugEnabled()) {
            logger.debug("useFlumeEventFormat set to: {}", Boolean.valueOf(this.useAvroEventFormat));
        }
        this.kafkaFutures = new LinkedList();

        String bootStrapServers = context.getString("kafka.bootstrap.servers");
        if ((bootStrapServers == null) || (bootStrapServers.isEmpty())) {
            throw new ConfigurationException("Bootstrap Servers must be specified");
        }
        setProducerProps(context, bootStrapServers);
        if ((logger.isDebugEnabled()) && (LogPrivacyUtil.allowLogPrintConfig())) {
            logger.debug("Kafka producer properties: {}", this.kafkaProps);
        }
        if (this.counter == null) {
            this.counter = new KafkaSinkCounter(getName());
        }
    }

    private void translateOldProps(Context ctx)
    {
        if (!ctx.containsKey("kafka.topic"))
        {
            ctx.put("kafka.topic", ctx.getString("topic"));
            logger.warn("{} is deprecated. Please use the parameter {}", "topic", "kafka.topic");
        }
        if (!ctx.containsKey("kafka.bootstrap.servers"))
        {
            String brokerList = ctx.getString("brokerList");
            if ((brokerList == null) || (brokerList.isEmpty())) {
                throw new ConfigurationException("Bootstrap Servers must be specified");
            }
            ctx.put("kafka.bootstrap.servers", brokerList);
            logger.warn("{} is deprecated. Please use the parameter {}", "brokerList", "kafka.bootstrap.servers");
        }
        if (!ctx.containsKey("flumeBatchSize"))
        {
            String oldBatchSize = ctx.getString("batchSize");
            if ((oldBatchSize != null) && (!oldBatchSize.isEmpty()))
            {
                ctx.put("flumeBatchSize", oldBatchSize);
                logger.warn("{} is deprecated. Please use the parameter {}", "batchSize", "flumeBatchSize");
            }
        }
        if (!ctx.containsKey("kafka.producer.acks"))
        {
            String requiredKey = ctx.getString("requiredAcks");
            if ((requiredKey != null) && (!requiredKey.isEmpty()))
            {
                ctx.put("kafka.producer.acks", requiredKey);
                logger.warn("{} is deprecated. Please use the parameter {}", "requiredAcks", "kafka.producer.acks");
            }
        }
        if (ctx.containsKey("key.serializer.class")) {
            logger.warn("{} is deprecated. Flume now uses the latest Kafka producer which implements a different interface for serializers. Please use the parameter {}", "key.serializer.class", "kafka.producer.key.serializer");
        }
        if (ctx.containsKey("serializer.class")) {
            logger.warn("{} is deprecated. Flume now uses the latest Kafka producer which implements a different interface for serializers. Please use the parameter {}", "serializer.class", "kafka.producer.value.serializer");
        }
    }

    private void setProducerProps(Context context, String bootStrapServers)
    {
        this.kafkaProps.clear();
        this.kafkaProps.put("acks", "1");

        this.kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.kafkaProps.putAll(context.getSubProperties("kafka.producer."));
        this.kafkaProps.put("bootstrap.servers", bootStrapServers);
    }

    protected Properties getKafkaProps()
    {
        return this.kafkaProps;
    }

    private byte[] serializeEvent(Event event, boolean useAvroEventFormat)
            throws IOException
    {
        byte[] bytes;
        if (useAvroEventFormat)
        {
            if (!this.tempOutStream.isPresent()) {
                this.tempOutStream = Optional.of(new ByteArrayOutputStream());
            }
            if (!this.writer.isPresent()) {
                this.writer = Optional.of(new SpecificDatumWriter(AvroFlumeEvent.class));
            }
            ((ByteArrayOutputStream)this.tempOutStream.get()).reset();
            AvroFlumeEvent e = new AvroFlumeEvent(toCharSeqMap(event.getHeaders()), ByteBuffer.wrap(event.getBody()));

            this.encoder = EncoderFactory.get().directBinaryEncoder((OutputStream)this.tempOutStream.get(), this.encoder);
            ((SpecificDatumWriter)this.writer.get()).write(e, this.encoder);
            this.encoder.flush();
            bytes = ((ByteArrayOutputStream)this.tempOutStream.get()).toByteArray();
        }
        else
        {
            bytes = event.getBody();
        }
        return bytes;
    }

    private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap)
    {
        Map<CharSequence, CharSequence> charSeqMap = new HashMap();
        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            charSeqMap.put(entry.getKey(), entry.getValue());
        }
        return charSeqMap;
    }
}
