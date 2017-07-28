package office;

/**
 * Created by Hu Feng on 2017/7/21.
 */
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SinkCallback
        implements Callback
{
    private static final Logger logger = LoggerFactory.getLogger(SinkCallback.class);
    private long startTime;

    public SinkCallback(long startTime)
    {
        this.startTime = startTime;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception)
    {
        if (exception != null) {
            logger.debug("Error sending message to Kafka {} ", exception.getMessage());
        }
        if (logger.isDebugEnabled())
        {
            long eventElapsedTime = System.currentTimeMillis() - this.startTime;
            logger.debug("Acked message partition:{} ofset:{}", Integer.valueOf(metadata.partition()), Long.valueOf(metadata.offset()));
            logger.debug("Elapsed time for send: {}", Long.valueOf(eventElapsedTime));
        }
    }
}
