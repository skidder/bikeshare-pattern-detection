package com.github.skidder.gbfs.processor;

import com.github.skidder.gbfs.processor.serialization.ProtobufV3Serializer;
import com.github.skidder.gbfs.processor.sink.GBFSStationStatusInfluxDBSink;
import com.github.skidder.gbfs.processor.sink.LoggerSink;
import com.github.skidder.gbfs.processor.source.RabbitMQSource;
import com.github.skidder.gbfs.proto.Gbfs;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws ClassNotFoundException {
        // Execution Environment Setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.minutes(1)));

        // required so that Protobuf types can be serialized with Kryo
        // https://stackoverflow.com/questions/32453030/using-an-collectionsunmodifiablecollection-with-apache-flink
        env.getConfig().addDefaultKryoSerializer(Class.forName("com.google.protobuf.GeneratedMessageV3"), ProtobufV3Serializer.class);

        final DataStream<Gbfs.CompleteGBFSMessage> source = new RabbitMQSource().start(env);
        source.addSink(new GBFSStationStatusInfluxDBSink());
        source.addSink(new LoggerSink<>());

        try {
//            System.out.println(env.getExecutionPlan());
            env.execute();
        } catch (Exception e) {
            LOG.error("Error during execution", e);
        }
    }
}
