/*
 * Copyright 2024 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.external;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.netflix.conductor.core.config.ConductorProperties;

@Component
public class WorkflowExternalBeans {

    public static final String EXECUTOR_ASYNC_DAO = "EXEC_ASYNC_DAO";
    public static final String EXECUTOR_ASYNC_LISTENER = "EXEC_ASYNC_LISTENER";
    public static final String EXECUTOR_ASYNC_START = "EXEC_ASYNC_START";
    private static final String LOCKED_QUEUE = "_lockedQueue.";
    private static final String LOCKED_CONFLICT_QUEUE = "_lockedQueue.conflict";

    private ExecutorService newFixedExecutor(String pattern, int threads) {
        return Executors.newFixedThreadPool(
                threads,
                new BasicThreadFactory.Builder()
                        .namingPattern(pattern + "-%d")
                        .daemon(true)
                        .build());
    }

    @Bean
    @Qualifier(EXECUTOR_ASYNC_DAO)
    public ExecutorService asyncDaoExecutor(ConductorProperties properties) {
        return newFixedExecutor("exec-async-dao", properties.getAsyncDaoThreadCount());
    }

    @Bean
    @Qualifier(EXECUTOR_ASYNC_LISTENER)
    public ExecutorService asyncListenerExecutor(ConductorProperties properties) {
        return newFixedExecutor("exec-async-listener", properties.getAsyncListenerThreadCount());
    }

    @Bean
    @Qualifier(EXECUTOR_ASYNC_START)
    public ExecutorService asyncStartExecutor(ConductorProperties properties) {
        return newFixedExecutor("exec-async-start", properties.getAsyncStartThreadCount());
    }

    public static String lockedQueue(String workflowId) {
        return LOCKED_QUEUE + workflowId;
    }
}
