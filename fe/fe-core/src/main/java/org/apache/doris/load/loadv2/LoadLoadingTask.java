// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TabletCommitInfo;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class LoadLoadingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(LoadLoadingTask.class);

    /*
     * load id is used for plan.
     * It should be changed each time we retry this load plan
     */
    private TUniqueId loadId;
    private final Database db;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final long jobDeadlineMs;
    private final long execMemLimit;
    private final boolean strictMode;
    private final boolean isPartialUpdate;
    private final long txnId;
    private final String timezone;
    // timeout of load job, in seconds
    private final long timeoutS;
    private final int loadParallelism;
    private final int sendBatchParallelism;
    private final boolean loadZeroTolerance;
    private final boolean singleTabletLoadPerSink;
    private final boolean useNewLoadScanNode;

    private final boolean enableMemTableOnSinkNode;

    private LoadingTaskPlanner planner;

    private Profile jobProfile;
    private long beginTime;
    private String clusterId;

    private List<TPipelineWorkloadGroup> tWorkloadGroups = null;

    public LoadLoadingTask(Database db, OlapTable table,
            BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
            long jobDeadlineMs, long execMemLimit, boolean strictMode, boolean isPartialUpdate,
            long txnId, LoadTaskCallback callback, String timezone,
            long timeoutS, int loadParallelism, int sendBatchParallelism,
            boolean loadZeroTolerance, Profile jobProfile, boolean singleTabletLoadPerSink,
            boolean useNewLoadScanNode, Priority priority, boolean enableMemTableOnSinkNode) {
        super(callback, TaskType.LOADING, priority);
        this.db = db;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.jobDeadlineMs = jobDeadlineMs;
        this.execMemLimit = execMemLimit;
        this.strictMode = strictMode;
        this.isPartialUpdate = isPartialUpdate;
        this.txnId = txnId;
        this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL);
        this.retryTime = 2; // 2 times is enough
        this.timezone = timezone;
        this.timeoutS = timeoutS;
        this.loadParallelism = loadParallelism;
        this.sendBatchParallelism = sendBatchParallelism;
        this.loadZeroTolerance = loadZeroTolerance;
        this.jobProfile = jobProfile;
        this.singleTabletLoadPerSink = singleTabletLoadPerSink;
        this.useNewLoadScanNode = useNewLoadScanNode;
        this.enableMemTableOnSinkNode = enableMemTableOnSinkNode;
    }

    public void init(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusList,
                     int fileNum, UserIdentity userInfo) throws UserException {
        this.loadId = loadId;
        planner = new LoadingTaskPlanner(callback.getCallbackId(), txnId, db.getId(), table, brokerDesc, fileGroups,
                strictMode, isPartialUpdate, timezone, this.timeoutS, this.loadParallelism, this.sendBatchParallelism,
                this.useNewLoadScanNode, userInfo, singleTabletLoadPerSink);
        planner.plan(loadId, fileStatusList, fileNum);
    }

    public void init(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusList,
            int fileNum, UserIdentity userInfo, String clusterId) throws UserException {
        this.loadId = loadId;
        planner = new LoadingTaskPlanner(callback.getCallbackId(), txnId, db.getId(), table, brokerDesc, fileGroups,
                strictMode, isPartialUpdate, timezone, this.timeoutS, this.loadParallelism, this.sendBatchParallelism,
                this.useNewLoadScanNode, userInfo, singleTabletLoadPerSink);
        boolean needCleanCtx = false;
        try {
            if (Config.isCloudMode()) {
                String clusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                        .getClusterNameByClusterId(clusterId);
                if (Strings.isNullOrEmpty(clusterName)) {
                    String errMsg = "cluster name is empty, cluster id is " + clusterId;
                    LOG.warn(errMsg);
                    throw new UserException(errMsg);
                }

                if (ConnectContext.get() == null) {
                    ConnectContext ctx = new ConnectContext();
                    ctx.setThreadLocalInfo();
                    ctx.setCloudCluster(clusterName);
                    needCleanCtx = true;
                } else {
                    ConnectContext.get().setCloudCluster(clusterName);
                }
            }
            planner.plan(loadId, fileStatusList, fileNum);
            this.clusterId = clusterId;
        } catch (Exception e) {
            throw e;
        } finally {
            if (Config.isCloudMode() && needCleanCtx) {
                ConnectContext.remove();
            }
        }
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    @Override
    protected void executeTask() throws Exception {
        LOG.info("begin to execute loading task. load id: {} job id: {}. db: {}, tbl: {}. left retry: {}",
                DebugUtil.printId(loadId), callback.getCallbackId(), db.getFullName(), table.getName(), retryTime);
        boolean needCleanCtx = false;
        if (Config.isCloudMode()) {
            String clusterName = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                    .getClusterNameByClusterId(this.clusterId);
            if (Strings.isNullOrEmpty(clusterName)) {
                throw new Exception("cluster is empty");
            }

            if (ConnectContext.get() == null) {
                ConnectContext ctx = new ConnectContext();
                ctx.setThreadLocalInfo();
                ctx.setCloudCluster(clusterName);
                needCleanCtx = true;
            } else {
                ConnectContext.get().setCloudCluster(clusterName);
            }
        }

        retryTime--;
        beginTime = System.currentTimeMillis();
        if (!((BrokerLoadJob) callback).updateState(JobState.LOADING)) {
            // job may already be cancelled
            return;
        }
        executeOnce();

        if (Config.isCloudMode() && needCleanCtx) {
            ConnectContext.remove();
        }
    }

    protected void executeOnce() throws Exception {
        // New one query id,
        Coordinator curCoordinator =  EnvFactory.getInstance().createCoordinator(callback.getCallbackId(),
                loadId, planner.getDescTable(),
                planner.getFragments(), planner.getScanNodes(), planner.getTimezone(), loadZeroTolerance);
        if (this.jobProfile != null) {
            this.jobProfile.addExecutionProfile(curCoordinator.getExecutionProfile());
        }
        curCoordinator.setQueryType(TQueryType.LOAD);
        curCoordinator.setExecMemoryLimit(execMemLimit);
        curCoordinator.setExecPipEngine(Config.enable_pipeline_load);

        /*
         * For broker load job, user only need to set mem limit by 'exec_mem_limit' property.
         * And the variable 'load_mem_limit' does not make any effect.
         * However, in order to ensure the consistency of semantics when executing on the BE side,
         * and to prevent subsequent modification from incorrectly setting the load_mem_limit,
         * here we use exec_mem_limit to directly override the load_mem_limit property.
         */
        curCoordinator.setLoadMemLimit(execMemLimit);
        curCoordinator.setMemTableOnSinkNode(enableMemTableOnSinkNode);

        long leftTimeMs = getLeftTimeMs();
        if (leftTimeMs <= 0) {
            throw new LoadException("failed to execute loading task when timeout");
        }
        // 1 second is the minimum granularity of actual execution
        int timeoutS = Math.max((int) (leftTimeMs / 1000), 1);
        curCoordinator.setTimeout(timeoutS);

        if (tWorkloadGroups != null) {
            curCoordinator.setTWorkloadGroups(tWorkloadGroups);
        }

        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, new QeProcessorImpl.QueryInfo(curCoordinator));
            actualExecute(curCoordinator, timeoutS);
            if (this.jobProfile != null) {
                curCoordinator.getExecutionProfile().update(beginTime, true);
            }
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }
    }

    private void actualExecute(Coordinator curCoordinator, int waitSecond) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("task_id", signature)
                    .add("query_id", DebugUtil.printId(curCoordinator.getQueryId()))
                    .add("msg", "begin to execute plan")
                    .build());
        }
        curCoordinator.exec();
        if (curCoordinator.join(waitSecond)) {
            Status status = curCoordinator.getExecStatus();
            if (status.ok()) {
                attachment = new BrokerLoadingTaskAttachment(signature,
                        curCoordinator.getLoadCounters(),
                        curCoordinator.getTrackingUrl(),
                        TabletCommitInfo.fromThrift(curCoordinator.getCommitInfos()),
                        ErrorTabletInfo.fromThrift(curCoordinator.getErrorTabletInfos()
                                .stream().limit(Config.max_error_tablet_of_broker_load).collect(Collectors.toList())));
                curCoordinator.getErrorTabletInfos().clear();
            } else {
                throw new LoadException(status.getErrorMsg());
            }
        } else {
            throw new LoadException("coordinator could not finished before job timeout");
        }
    }

    public long getLeftTimeMs() {
        return jobDeadlineMs - System.currentTimeMillis();
    }

    @Override
    public void updateRetryInfo() {
        super.updateRetryInfo();
        UUID uuid = UUID.randomUUID();
        this.loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        planner.updateLoadId(this.loadId);
    }

    void settWorkloadGroups(List<TPipelineWorkloadGroup> tWorkloadGroups) {
        this.tWorkloadGroups = tWorkloadGroups;
    }

}
