package org.apache.flink.api.common.serialization;

import java.io.*;
import java.util.zip.*;

public class GzipDeserializationSchema extends AbstractDeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ByteArrayInputStream bis = new ByteArrayInputStream(message);
        GZIPInputStream in = new GZIPInputStream(bis);
        byte[] buffer = new byte[1024];
        int len;
        while ((len = in.read(buffer)) >= 0) {
            bos.write(buffer, 0, len);
        }
        in.close();
        bos.close();
        return bos.toByteArray();
    }
}

