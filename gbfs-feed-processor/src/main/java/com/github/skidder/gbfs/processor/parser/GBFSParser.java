package com.github.skidder.gbfs.processor.parser;

import com.github.skidder.gbfs.proto.Gbfs;
import com.google.protobuf.Parser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GBFSParser
        implements FlatMapFunction<byte[], Gbfs.CompleteGBFSMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(GBFSParser.class);

    private static final long serialVersionUID = 475753463711582700L;
    private transient Parser<Gbfs.CompleteGBFSMessage> parser;

    @Override
    public void flatMap(byte[] value, Collector<Gbfs.CompleteGBFSMessage> out) throws Exception {
        if (parser == null) {
            parser = Gbfs.CompleteGBFSMessage.parser();
        }
        Gbfs.CompleteGBFSMessage sourceMessage;
        try {
            sourceMessage = parser.parseFrom(value);
        } catch (Exception e) {
            LOG.error("Error parsing message from Protobuf", e);
            return;
        }
        out.collect(sourceMessage);
    }
}
