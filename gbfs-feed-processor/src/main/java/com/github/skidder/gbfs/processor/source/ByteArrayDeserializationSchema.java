package com.github.skidder.gbfs.processor.source;

import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * Schema for deserialization of raw bytes. Used for the retrieval of
 * protobuf-encoded messages.
 */
public class ByteArrayDeserializationSchema extends AbstractDeserializationSchema<byte[]> {

    private static final long serialVersionUID = -6016430431690601901L;

    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        return message;
    }
}
