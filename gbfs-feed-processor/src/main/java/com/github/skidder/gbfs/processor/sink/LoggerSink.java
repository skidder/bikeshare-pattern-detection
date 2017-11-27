package com.github.skidder.gbfs.processor.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerSink<IN> extends RichSinkFunction<IN> {
    private static Logger LOG = LoggerFactory.getLogger(LoggerSink.class);

    @Override
    public void invoke(IN in) throws Exception {
        LOG.info(in.toString());
    }
}
