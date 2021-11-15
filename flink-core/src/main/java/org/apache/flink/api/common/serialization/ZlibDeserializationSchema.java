package org.apache.flink.api.common.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class ZlibDeserializationSchema extends AbstractDeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        Inflater decompressor = new Inflater();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        decompressor.setInput(message);
        byte[] buffer = new byte[1024];
        int len=0;
        do {
            try {
                len = decompressor.inflate(buffer);
            } catch (DataFormatException e) {
                e.printStackTrace();
            }
            bos.write(buffer, 0, len);
        } while (len > 0);
        decompressor.end();
        bos.close();
        return bos.toByteArray();
    }
}

