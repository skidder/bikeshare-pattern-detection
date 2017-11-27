package com.github.skidder.gbfs.processor.source;

import java.io.Serializable;
import java.io.Serializable;

import com.github.skidder.gbfs.processor.parser.GBFSParser;
import com.github.skidder.gbfs.proto.Gbfs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;


public class RabbitMQSource implements Serializable {

    private static final long serialVersionUID = 1841646030347808417L;

    /**
     * Creates a stream source from the RabbitMQ queue specified in the
     * application config.
     *
     * @param env Flink stream execution environment to associated the source
     *            with
     * @return RabbitMQ stream source for control-messages.
     */
    public DataStream<Gbfs.CompleteGBFSMessage> start(StreamExecutionEnvironment env) {
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("rabbitmq")
                .setPort(5672)
                .setAutomaticRecovery(true)
                .setTopologyRecoveryEnabled(true)
                .setVirtualHost("/")
                .setUserName("guest")
                .setPassword("guest")
                .build();

        return env
                .addSource(new RMQSource<>(connectionConfig, "gbfs-feed",
                        new ByteArrayDeserializationSchema()))
                .uid("rabbitmq-control-source")
                .name("RabbitMQ Control Source")
                .flatMap(new GBFSParser())
                .name("Parse")
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Gbfs.CompleteGBFSMessage>() {
                    private static final long serialVersionUID = -3283830718224855053L;
                    // maximum observed timestamp
                    private long maxTimestamp;

                    @Override
                    public long extractTimestamp(Gbfs.CompleteGBFSMessage element, long previousElementTimestamp) {
                        // track the maximum observed timestamp in the
                        // input stream
                        maxTimestamp = Math.max(maxTimestamp, element.getStationStatusMessage().getLastUpdated());
                        return element.getStationStatusMessage().getLastUpdated();
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTimestamp);
                    }
                })
                .name("Timestamp and Watermark");
    }


}
