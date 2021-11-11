package org.apache.flink.api.common.serialization;

/**
 * This class simply allows the serialized bytes to be passed directly out of the
 */
public class RawDeserializationSchema extends AbstractDeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message){
        return message;
    }
}
