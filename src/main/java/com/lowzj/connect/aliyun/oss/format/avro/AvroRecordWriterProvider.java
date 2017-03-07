/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lowzj.connect.aliyun.oss.format.avro;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lowzj.connect.aliyun.oss.storage.AliyunOSSOutputStream;
import com.lowzj.connect.aliyun.oss.AliyunOSSSinkConnectorConfig;
import com.lowzj.connect.aliyun.oss.storage.AliyunOSSStorage;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class AvroRecordWriterProvider implements RecordWriterProvider<AliyunOSSSinkConnectorConfig> {

    private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
    private static final String EXTENSION = ".avro";
    private final AliyunOSSStorage storage;
    private final AvroData avroData;

    AvroRecordWriterProvider(AliyunOSSStorage storage, AvroData avroData) {
        this.storage = storage;
        this.avroData = avroData;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter getRecordWriter(final AliyunOSSSinkConnectorConfig conf, final String filename) {
        // This is not meant to be a thread-safe writer!
        return new RecordWriter() {
            final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
            Schema schema = null;
            AliyunOSSOutputStream ossOut;

            @Override
            public void write(SinkRecord record) {
                if (schema == null) {
                    schema = record.valueSchema();
                    try {
                        log.info("Opening record writer for: {}", filename);
                        ossOut = storage.create(filename, true);
                        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
                        writer.create(avroSchema, ossOut);
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
                log.trace("Sink record: {}", record);
                Object value = avroData.fromConnectData(schema, record.value());
                try {
                    writer.append(value);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void commit() {
                try {
                    // Flush is required here, because closing the writer will close the underlying Aliyun OSS output stream before
                    // committing any data to Aliyun OSS.
                    writer.flush();
                    ossOut.commit();
                    writer.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void close() {
                try {
                    writer.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }
        };
    }
}
