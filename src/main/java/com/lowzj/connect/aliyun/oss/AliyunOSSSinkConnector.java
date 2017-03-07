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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;

import com.lowzj.connect.aliyun.oss.util.Version;

import lombok.extern.slf4j.Slf4j;

/**
 * Connector class for Aliyun OSS
 */
@Slf4j
public class AliyunOSSSinkConnector extends Connector {
    private Map<String, String> configProps;
    private AliyunOSSSinkConnectorConfig config;

    /**
     * No-arg constructor. It is instantiated by Connect framework.
     */
    public AliyunOSSSinkConnector() {
        // no-arg constructor required by Connect framework.
    }

    // Visible for testing.
    AliyunOSSSinkConnector(AliyunOSSSinkConnectorConfig config) {
        this.config = config;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = new HashMap<>(props);
        config = new AliyunOSSSinkConnectorConfig(props);
        log.info("Starting Aliyun OSS connector {}", config.getName());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AliyunOSSSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(configProps);
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Shutting down Aliyun OSS connector {}", config.getName());
    }

    @Override
    public ConfigDef config() {
        return AliyunOSSSinkConnectorConfig.getConfig();
    }

}
