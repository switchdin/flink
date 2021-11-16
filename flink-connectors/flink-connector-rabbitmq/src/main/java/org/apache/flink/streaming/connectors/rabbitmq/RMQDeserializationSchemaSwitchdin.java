/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RawDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 * A {@link RMQDeserializationSchema} implementation that uses the {@link
 * RawDeserializationSchema} to return the routing key and the raw bytes from the message.
 */
final class RMQDeserializationSchemaSwitchdin implements RMQDeserializationSchema<Tuple2<String, byte[]>> {
    private static final TypeInformation<Tuple2<String, byte[]>> typeInfo = new TypeHint<Tuple2<String, byte[]>>(){}.getTypeInfo();
    private final DeserializationSchema<byte[]> schema = new RawDeserializationSchema();

    public RMQDeserializationSchemaSwitchdin() {
    }

    @Override
    public void deserialize(
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body,
            RMQCollector<Tuple2<String, byte[]>> collector)
            throws IOException {
        collector.collect(new Tuple2<>(envelope.getRoutingKey(), schema.deserialize(body)));
    }

    @Override
    public TypeInformation<Tuple2<String, byte[]>> getProducedType() {
        return RMQDeserializationSchemaSwitchdin.typeInfo;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        schema.open(context);
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, byte[]> nextElement) {
        return false;
    }
}
