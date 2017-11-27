package com.github.skidder.gbfs.processor.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.GeneratedMessageV3;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.esotericsoftware.kryo.Kryo.NULL;

public class ProtobufV3Serializer extends Serializer<GeneratedMessageV3> {
    private Method parseFromMethod = null;

    @Override
    public void write(Kryo kryo, Output output, GeneratedMessageV3 protobufMessage) {
        // If our protobuf is null
        if (protobufMessage == null) {
            // Write our special null value
            output.writeByte(NULL);
            output.flush();

            // and we're done
            return;
        }

        // Otherwise serialize protobuf to a byteArray
        byte[] bytes = protobufMessage.toByteArray();

        // Write the length of our byte array
        output.writeInt(bytes.length + 1, true);

        // Write the byte array out
        output.writeBytes(bytes);
        output.flush();
    }

    @Override
    public GeneratedMessageV3 read(Kryo kryo, Input input, Class<GeneratedMessageV3> type) {
        // Read the length of our byte array
        int length = input.readInt(true);

        // If the length is equal to our special null value
        if (length == NULL) {
            // Just return null
            return null;
        }
        // Otherwise read the byte array length
        byte[] bytes = input.readBytes(length - 1);
        try {
            // Deserialize protobuf
            return (GeneratedMessageV3) (getParseFromMethod(type).invoke(type, bytes));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unable to deserialize protobuf " + e.getMessage(), e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Unable to deserialize protobuf " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to deserialize protobuf " + e.getMessage(), e);
        }
    }

    /**
     * Caches method reflection lookup
     *
     * @throws NoSuchMethodException
     */
    private Method getParseFromMethod(Class<GeneratedMessageV3> type) throws NoSuchMethodException {
        if (parseFromMethod == null) {
            parseFromMethod = type.getMethod("parseFrom", byte[].class);
        }
        return parseFromMethod;
    }

    @Override
    public boolean getAcceptsNull() {
        return true;
    }
}

