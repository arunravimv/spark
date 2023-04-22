/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.shuffle.checksum.Cause;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.util.TimerWithCustomTimeUnit;
import org.apache.spark.network.util.TransportConf;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class NFSBlockStoreClient extends ExternalBlockStoreClient{
    private ExecutorService threadPoolExecutor;
    private String localDir;
    private String nfsShuffleManagerDataDir;

    private NFSBlockStoreMetrics nfsBlockStoreMetrics;

    private ObjectMapper objectMapper = new ObjectMapper();

    final ConcurrentMap<String, ExecutorShuffleInfo> executors = Maps.newConcurrentMap();
    /**
     * Creates an external shuffle client, with SASL optionally enabled. If SASL is not enabled,
     * then secretKeyHolder may be null.
     *
     * @param conf
     * @param secretKeyHolder
     * @param authEnabled
     * @param registrationTimeoutMs
     */
    public NFSBlockStoreClient(String localDir, TransportConf conf, SecretKeyHolder secretKeyHolder, boolean authEnabled, long registrationTimeoutMs, int numUsableCores) {
        super(conf, secretKeyHolder, authEnabled, registrationTimeoutMs);
        this.localDir = localDir;
        this.threadPoolExecutor = Executors.newFixedThreadPool(Math.max(conf.clientThreads(),numUsableCores));
        this.nfsBlockStoreMetrics = new NFSBlockStoreMetrics();
    }

    @Override
    public void getHostLocalDirs(String host, int port, String[] execIds, CompletableFuture<Map<String, String[]>> hostLocalDirsCompletable) {
        logger.debug(String.format("Getting host local dirs for %s", Arrays.toString(execIds)));
        threadPoolExecutor.submit(()->{
            try {
                loadMissingExecutorInfo(findMissingExecs(execIds));
                logger.debug(String.format("host local executors identified : %s", String.join(",", executors.keySet())));
                Map<String, String[]> hostLocalDirs = Arrays.stream(execIds).sequential()
                        .map(execId -> {
                            ExecutorShuffleInfo info = executors.get(execId);
                            if (info == null) {
                                throw new RuntimeException(
                                        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
                            }
                            return Pair.of(execId, info.localDirs);
                        })
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                hostLocalDirsCompletable.complete(hostLocalDirs);
            }catch (RuntimeException e){
                hostLocalDirsCompletable.completeExceptionally(e);
            }
        });
    }

    @Override
    public void init(String appId) {
        logger.info(String.format("Initializing NFSBlockStoreClient with (appId: %s, localDir: %s)", appId, localDir));
        this.appId = appId;
        try {
            this.nfsShuffleManagerDataDir = new StringBuffer(localDir).append("/")
                    .append(appId).append("/").append("nfs_shuffle_manager").toString();
            File directory = new File(this.nfsShuffleManagerDataDir);
            Files.createDirectories(directory.toPath());
            if ( !directory.exists() || !directory.isDirectory()) {
                logger.error(String.format("Failed to create directory %s", this.nfsShuffleManagerDataDir ));
            }
            loadExecutorInfoFromBlockStoreState();
        } catch (Exception e) {
            logger.debug(String.format("Failed Initializing NFSBlockStoreClient with (appId: %s, localDir: %s) due to %s", appId, localDir, e.getMessage()));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerWithShuffleServer(String host, int port, String execId, ExecutorShuffleInfo executorInfo) throws IOException, InterruptedException {
        final Timer.Context executionDelayContext =
                nfsBlockStoreMetrics.getRegisterExecutorRequestLatencyMillis().time();
        try {
            logger.info(String.format("Register ExecutorShuffleInfo for %s using NFSBlockStoreClient(%s,%d)", execId, host, port));
            objectMapper.writeValueAsString(executorInfo);
            ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
            String execInfoFilename = new StringBuffer(nfsShuffleManagerDataDir).append("/").append(execId).toString();
            executors.put(execId, executorInfo);
            objectWriter.writeValue(new File(execInfoFilename), executorInfo);
        }finally {
            executionDelayContext.stop();
        }
    }

    @Override
    public Future<Integer> removeBlocks(String host, int port, String execId, String[] blockIds) throws IOException, InterruptedException {
        String [] execIds = {execId};
        return threadPoolExecutor.submit(()->{
                    loadMissingExecutorInfo(findMissingExecs(execIds));
                    ExecutorShuffleInfo executor = executors.get(execId);
                    if (executor == null) {
                        throw new RuntimeException(
                                String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
                    }
                    int numRemovedBlocks = 0;
                    for (String blockId : blockIds) {
                        File file = new File(
                                ExecutorDiskUtils.getFilePath(executor.localDirs, executor.subDirsPerLocalDir, blockId));
                        if (file.delete()) {
                            numRemovedBlocks++;
                        } else {
                            logger.warn("Failed to delete block: " + file.getAbsolutePath());
                        }
                    }
                    return numRemovedBlocks;
                });
    }

    @Override
    public Cause diagnoseCorruption(String host, int port, String execId, int shuffleId, long mapId, int reduceId, long checksum, String algorithm) {
        throw new NoSuchMethodError("diagnoseCorruption is not available for NFSBlockStoreClient ");

    }

    @Override
    protected void checkInit() {
        super.checkInit();
    }

    @Override
    public void setAppAttemptId(String appAttemptId) {
        super.setAppAttemptId(appAttemptId);
    }

    @Override
    public String getAppAttemptId() {
        return super.getAppAttemptId();
    }

    @Override
    public void fetchBlocks(String host, int port, String execId, String[] blockIds, BlockFetchingListener listener, DownloadFileManager downloadFileManager) {
        throw new NoSuchMethodError("fetchBlocks is not available for NFSBlockStoreClient ");
    }

    @Override
    public void pushBlocks(String host, int port, String[] blockIds, ManagedBuffer[] buffers, BlockPushingListener listener) {
        throw new NoSuchMethodError("pushBlocks is not available for NFSBlockStoreClient ");
    }

    @Override
    public void finalizeShuffleMerge(String host, int port, int shuffleId, int shuffleMergeId, MergeFinalizerListener listener) {
        throw new NoSuchMethodError("finalizeShuffleMerge is not available for NFSBlockStoreClient ");
    }

    @Override
    public void getMergedBlockMeta(String host, int port, int shuffleId, int shuffleMergeId, int reduceId, MergedBlocksMetaListener listener) {
        throw new NoSuchMethodError("getMergedBlockMeta is not available for NFSBlockStoreClient ");
    }

    @Override
    public MetricSet shuffleMetrics() {
       return nfsBlockStoreMetrics;
    }

    @Override
    public void close() {
        threadPoolExecutor.shutdown();
    }

    private void loadExecutorInfoFromBlockStoreState() {
        File directory = new File(this.nfsShuffleManagerDataDir);
        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isFile()) {
                String execId = file.getName();
                try {
                    ExecutorShuffleInfo executorShuffleInfo = objectMapper.readValue(file, ExecutorShuffleInfo.class);
                    executors.put(execId, executorShuffleInfo);
                }catch (StreamReadException e) {
                    throw new RuntimeException(e);
                } catch (DatabindException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void loadMissingExecutorInfo(ArrayList<String> execIds) {
        for(String execId : execIds){
            String filename = new StringBuffer(this.nfsShuffleManagerDataDir).append("/").append(execId).toString();
            File file = new File(filename);
            if (file.isFile()) {
                try {
                    executors.put(execId, objectMapper.readValue(file, ExecutorShuffleInfo.class));
                } catch (IOException e) {
                    logger.debug("Failed to load executor ({}) block info due to {}", execId, e);
                    throw new RuntimeException(e);
                }
            }
        }

    }

    private ArrayList<String>  findMissingExecs(String[] execIds) {
        ArrayList<String> missingExecIds = new ArrayList<>();
        for(String execId : execIds){
            if(!executors.containsKey(execId)) {
                missingExecIds.add(execId);
            }
        }
        logger.debug("Missing Executor IDs {}. Loading Executor details from state", missingExecIds);
        return missingExecIds;
    }

    public class NFSBlockStoreMetrics implements MetricSet {

        private final Map<String, Metric> allMetrics;
        private final Timer registerExecutorRequestLatencyMillis =
                new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);

        public NFSBlockStoreMetrics() {
            allMetrics = new HashMap<>();
            allMetrics.put("registerExecutorRequestLatencyMillis", registerExecutorRequestLatencyMillis);
        }

        @Override
        public Map<String, Metric> getMetrics() {
            return this.allMetrics;
        }

        public Timer getRegisterExecutorRequestLatencyMillis() {
            return registerExecutorRequestLatencyMillis;
        }
    }
}
