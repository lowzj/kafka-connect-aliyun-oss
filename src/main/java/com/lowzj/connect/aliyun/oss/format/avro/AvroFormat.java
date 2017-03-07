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

import com.lowzj.connect.aliyun.oss.AliyunOSSSinkConnectorConfig;
import com.lowzj.connect.aliyun.oss.storage.AliyunOSSStorage;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;

public class AvroFormat implements Format<AliyunOSSSinkConnectorConfig, String> {
    private final AliyunOSSStorage storage;
    private final AvroData avroData;

    public AvroFormat(AliyunOSSStorage storage) {
        this.storage = storage;
        this.avroData = new AvroData(storage.conf().getInt(AliyunOSSSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG));
    }

    @Override
    public RecordWriterProvider<AliyunOSSSinkConnectorConfig> getRecordWriterProvider() {
        return new AvroRecordWriterProvider(storage, avroData);
    }

    @Override
    public SchemaFileReader<AliyunOSSSinkConnectorConfig, String> getSchemaFileReader() {
        throw new UnsupportedOperationException("Reading schemas from Aliyun OSS is not currently supported");
    }

    @Override
    public HiveFactory getHiveFactory() {
        throw new UnsupportedOperationException("Hive integration is not currently supported in Aliyun OSS Connector");
    }

    public AvroData getAvroData() {
        return avroData;
    }
}
