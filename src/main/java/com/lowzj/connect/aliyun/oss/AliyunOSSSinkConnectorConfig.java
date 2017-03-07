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

package com.lowzj.connect.aliyun.oss;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;

import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class AliyunOSSSinkConnectorConfig extends StorageSinkConnectorConfig {

    // Aliyun OSS endpoint
    public static final String ENDPOINT_CONFIG = "aliyun-oss.endpoint";

    // Aliyun OSS bucket
    public static final String BUCKET_CONFIG = "aliyun-oss.bucket.name";

    // server side encryption, only support PUT object, COPY, Initiate Multipart Upload
    public static final String SSEA_CONFIG = "aliyun-oss.ssea.name";
    // only support AES256
    public static final String SSEA_DEFAULT = "";

    public static final String PART_SIZE_CONFIG = "aliyun-oss.part.size";
    public static final int PART_SIZE_DEFAULT = 100 * 1024 * 1024;

    public static final String CREDENTIALS_PROVIDER_CLASS_CONFIG = "aliyun-oss.credentials.provider.class";
    public static final Class<? extends CredentialsProvider> CREDENTIALS_PROVIDER_CLASS_DEFAULT =
                    DefaultCredentialProvider.class;

    private final String name;

    private final StorageCommonConfig commonConfig;
    private final HiveConfig hiveConfig;
    private final PartitionerConfig partitionerConfig;

    private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
    private final Set<AbstractConfig> allConfigs = new HashSet<>();

    static {
        {
            final String group = "aliyun-oss";
            int orderInGroup = 0;

            CONFIG_DEF.define(ENDPOINT_CONFIG,
                    Type.STRING,
                    Importance.HIGH,
                    "The Aliyun OSS Endpoint.",
                    group,
                    ++orderInGroup,
                    Width.LONG,
                    "Aliyun OSS Endpoint");

            CONFIG_DEF.define(BUCKET_CONFIG,
                            Type.STRING,
                            Importance.HIGH,
                            "The Aliyun OSS Bucket.",
                            group,
                            ++orderInGroup,
                            Width.LONG,
                            "Aliyun OSS Bucket");

            CONFIG_DEF.define(PART_SIZE_CONFIG,
                            Type.INT,
                            PART_SIZE_DEFAULT,
                            new PartRange(),
                            Importance.HIGH,
                            "The Part Size in Aliyun OSS Multi-part Uploads.",
                            group,
                            ++orderInGroup,
                            Width.LONG,
                            "Aliyun OSS Part Size");

            CONFIG_DEF.define(CREDENTIALS_PROVIDER_CLASS_CONFIG,
                            Type.CLASS,
                            CREDENTIALS_PROVIDER_CLASS_DEFAULT,
                            new CredentialsProviderValidator(),
                            Importance.LOW,
                            "Credentials provider or provider chain to use for authentication to Aliyun. By default the "
                                            + " connector uses 'DefaultCredentialProvider'.",
                            group,
                            ++orderInGroup,
                            Width.LONG,
                            "Aliyun Credentials Provider Class");

            CONFIG_DEF.define(SSEA_CONFIG,
                            Type.STRING,
                            SSEA_DEFAULT,
                            Importance.LOW,
                            "The Aliyun OSS Server Side Encryption Algorithm.",
                            group,
                            ++orderInGroup,
                            Width.LONG,
                            "Aliyun OSS Server Side Encryption Algorithm");
        }
    }

    public AliyunOSSSinkConnectorConfig(Map<String, String> props) {
        this(CONFIG_DEF, props);
    }

    protected AliyunOSSSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
        commonConfig = new StorageCommonConfig(originalsStrings());
        hiveConfig = new HiveConfig(originalsStrings());
        partitionerConfig = new PartitionerConfig(originalsStrings());
        this.name = parseName(originalsStrings());
        addToGlobal(hiveConfig);
        addToGlobal(partitionerConfig);
        addToGlobal(commonConfig);
        addToGlobal(this);
    }

    private void addToGlobal(AbstractConfig config) {
        allConfigs.add(config);
        addConfig(config.values(), (ComposableConfig) config);
    }

    private void addConfig(Map<String, ?> parsedProps, ComposableConfig config) {
        for (String key : parsedProps.keySet()) {
            propertyToConfig.put(key, config);
        }
    }

    public String getBucketName() {
        return getString(BUCKET_CONFIG);
    }

    public String getSSEA() {
        return getString(SSEA_CONFIG);
    }

    public int getPartSize() {
        return getInt(PART_SIZE_CONFIG);
    }

    @SuppressWarnings("unchecked")
    public CredentialsProvider getCredentialsProvider() {
        try {
            return ((Class<? extends CredentialsProvider>)
                            getClass(AliyunOSSSinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG)).newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new ConnectException("Invalid class for: " + AliyunOSSSinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG, e);
        }
    }

    protected static String parseName(Map<String, String> props) {
        String nameProp = props.get("name");
        return nameProp != null ? nameProp : "aliyun-oss-sink";
    }

    public String getName() {
        return name;
    }

    @Override
    public Object get(String key) {
        ComposableConfig config = propertyToConfig.get(key);
        if (config == null) {
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        }
        return config == this ? super.get(key) : config.get(key);
    }

    public Map<String, ?> plainValues() {
        Map<String, Object> map = new HashMap<>();
        for (AbstractConfig config : allConfigs) {
            map.putAll(config.values());
        }
        return map;
    }

    private static class PartRange implements ConfigDef.Validator {
        // Aliyun OSS specific limit
        final int min = 5 * 1024 * 1024;
        // Connector specific
        final int max = Integer.MAX_VALUE;

        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, value, "Part size must be non-null");
            }
            Number number = (Number) value;
            if (number.longValue() < min) {
                throw new ConfigException(name, value, "Part size must be at least: " + min + " bytes (5MB)");
            }
            if (number.longValue() > max) {
                throw new ConfigException(name, value, "Part size must be no more: " + Integer.MAX_VALUE + " bytes (~2GB)");
            }
        }

        public String toString() {
            return "[" + min + ",...," + max + "]";
        }
    }

    private static class CredentialsProviderValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object provider) {
            if (provider != null && provider instanceof Class
                            && CredentialsProvider.class.isAssignableFrom((Class<?>) provider)) {
                return;
            }
            throw new ConfigException(name, provider, "Class must extend: " + CredentialsProvider.class);
        }

        @Override
        public String toString() {
            return "Any class implementing: " + CredentialsProvider.class;
        }
    }

    public static ConfigDef getConfig() {
        Map<String, ConfigDef.ConfigKey> everything = new HashMap<>(CONFIG_DEF.configKeys());
        everything.putAll(StorageCommonConfig.getConfig().configKeys());
        everything.putAll(PartitionerConfig.getConfig().configKeys());

        Set<String> blacklist = new HashSet<>();
        blacklist.add(StorageSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
        blacklist.add(StorageSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
        blacklist.add(StorageSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG);

        ConfigDef visible = new ConfigDef();
        for (ConfigDef.ConfigKey key : everything.values()) {
            if(!blacklist.contains(key.name)) {
                visible.define(key);
            }
        }
        return visible;
    }

    public static void main(String[] args) {
        System.out.println(getConfig().toEnrichedRst());
    }

}
