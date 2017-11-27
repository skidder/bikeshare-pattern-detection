package com.github.skidder.gbfs.processor;

import com.github.skidder.gbfs.processor.sink.LoggerSink;
import com.github.skidder.gbfs.processor.source.RabbitMQSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        // Execution Environment Setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.minutes(1)));

        new RabbitMQSource().start(env).addSink(new LoggerSink<>());

        try {
//            System.out.println(env.getExecutionPlan());
            env.execute();
        } catch (Exception e) {
            LOG.error("Error during execution", e);
        }
    }
}
