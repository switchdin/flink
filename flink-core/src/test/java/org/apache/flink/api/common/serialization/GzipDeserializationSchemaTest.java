package org.apache.flink.api.common.serialization;

import lombok.SneakyThrows;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class GzipDeserializationSchemaTest {
    @SneakyThrows
    @Test
    public void testGzipExtractionOfShortMessage() {
        final byte[] testData = {
                (byte) 0x1f, (byte) 0x8b, (byte) 0x8, (byte) 0x0, (byte) 0xf3,
                (byte) 0xb9, (byte) 0x8d, (byte) 0x61, (byte) 0x2, (byte) 0xff, (byte) 0xcb,
                (byte) 0x48, (byte) 0xcd, (byte) 0xc9, (byte) 0xc9, (byte) 0x57, (byte) 0x70,
                (byte) 0xcb, (byte) 0xc9, (byte) 0xcc, (byte) 0xcb, (byte) 0x56, (byte) 0x4,
                (byte) 0x0, (byte) 0x8a, (byte) 0x74, (byte) 0x36, (byte) 0x88, (byte) 0xc,
                (byte) 0x0, (byte) 0x0, (byte) 0x0
        };
        String resultString = "hello Flink!";
        byte[] expectedResultBytes = resultString.getBytes(StandardCharsets.UTF_8);
        GzipDeserializationSchema testSchema = new GzipDeserializationSchema();
        byte[] result = testSchema.deserialize(testData);
        assertEquals(expectedResultBytes, result);
    }
}
