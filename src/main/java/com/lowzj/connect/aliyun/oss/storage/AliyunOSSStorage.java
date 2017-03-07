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

package com.lowzj.connect.aliyun.oss.storage;

import java.io.OutputStream;

import org.apache.avro.file.SeekableInput;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectListing;
import com.lowzj.connect.aliyun.oss.AliyunOSSSinkConnectorConfig;

import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.common.util.StringUtils;

/**
 * Aliyun OSS implementation of the storage interface for Connect sinks.
 */
public class AliyunOSSStorage implements Storage<AliyunOSSSinkConnectorConfig, ObjectListing> {

    private final String url;
    private final String bucketName;
    private final OSSClient oss;
    private final AliyunOSSSinkConnectorConfig conf;
    private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 KafkaS3Connector/%s";

    /**
     * Construct an Aliyun OSS storage class given a configuration and an Aliyun OSS address.
     *
     * @param conf the OSS configuration.
     * @param url the OSS address.
     */
    public AliyunOSSStorage(AliyunOSSSinkConnectorConfig conf, String url) {
        this.url = url;
        this.conf = conf;
        this.bucketName = conf.getBucketName();
        this.oss = newOSSClient(conf);
    }

    public OSSClient newOSSClient(AliyunOSSSinkConnectorConfig config) {
        ClientConfiguration configuration = new ClientConfiguration();
//        OSSClient oss = new OSSClient(config.getString(ENDPOINT_CONFIG),
//                config.getCredentialsProvider(),
//                config);

        return null;
    }

    // Visible for testing.
    public AliyunOSSStorage(AliyunOSSSinkConnectorConfig conf, String url, String bucketName, OSSClient oss) {
        this.url = url;
        this.conf = conf;
        this.bucketName = bucketName;
        this.oss = oss;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(String name) {
        return StringUtils.isNotBlank(name) && oss.doesObjectExist(bucketName, name);
    }

    /**
     * {@inheritDoc}
     */
    public boolean bucketExists() {
        return StringUtils.isNotBlank(bucketName) && oss.doesBucketExist(bucketName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean create(String name) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * This method is not supported in S3 storage.
     *
     * @throws UnsupportedOperationException
     */
    @Override
    public OutputStream append(String filename) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String name) {
        if (bucketName.equals(name)) {
            // TODO: decide whether to support delete for the top-level bucket.
            // oss.deleteBucket(name);
            return;
        } else {
            oss.deleteObject(bucketName, name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectListing list(String path) {
        return oss.listObjects(bucketName, path);
    }

    @Override
    public AliyunOSSSinkConnectorConfig conf() { return conf; }

    @Override
    public String url() {
        return url;
    }

    @Override
    public SeekableInput open(String path, AliyunOSSSinkConnectorConfig conf) {
        throw new UnsupportedOperationException("File reading is not currently supported in S3 Connector");
    }

    @Override
    public OutputStream create(String path, AliyunOSSSinkConnectorConfig conf, boolean overwrite) {
        return create(path, overwrite);
    }

    public AliyunOSSOutputStream create(String path, boolean overwrite) {
        if (!overwrite) {
            throw new UnsupportedOperationException("Creating a file without overwriting is not currently supported in S3 Connector");
        }

        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Path can not be empty!");
        }

        // currently ignore what is passed as method argument.
        return new AliyunOSSOutputStream(path, this.conf, oss);
    }
}
