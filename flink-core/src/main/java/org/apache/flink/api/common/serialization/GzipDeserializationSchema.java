package org.apache.flink.api.common.serialization;

import java.io.*;
import java.util.zip.*;

public class GzipDeserializationSchema extends AbstractDeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        ByteArrayInputStream compressedStream = new ByteArrayInputStream(message);
        GZIPInputStream zipStream = new GZIPInputStream(compressedStream, message.length);
        byte[] decompressedData = new byte[message.length];
        zipStream.read(decompressedData);
        zipStream.close();
        compressedStream.close();
        return decompressedData;
    }
}

