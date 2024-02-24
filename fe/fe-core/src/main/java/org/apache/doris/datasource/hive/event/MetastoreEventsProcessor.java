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


package org.apache.doris.datasource.hive.event;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.hive.HMSClientException;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A metastore event is a instance of the class
 * {@link NotificationEvent}. Metastore can be
 * configured, to work with Listeners which are called on various DDL operations like
 * create/alter/drop operations on database, table, partition etc. Each event has a unique
 * incremental id and the generated events are be fetched from Metastore to get
 * incremental updates to the metadata stored in Hive metastore using the the public API
 * <code>get_next_notification</code> These events could be generated by external
 * Metastore clients like Apache Hive or Apache Spark configured to talk with the same metastore.
 * <p>
 * This class is used to poll metastore for such events at a given frequency. By observing
 * such events, we can take appropriate action on the {@link org.apache.doris.datasource.hive.HiveMetaStoreCache}
 * (refresh/invalidate/add/remove) so that represents the latest information
 * available in metastore. We keep track of the last synced event id in each polling
 * iteration so the next batch can be requested appropriately. The current batch size is
 * constant and set to {@link org.apache.doris.common.Config#hms_events_batch_size_per_rpc}.
 */
public class MetastoreEventsProcessor extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(MetastoreEventsProcessor.class);
    public static final String HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY =
            "hive.metastore.notifications.add.thrift.objects";

    // for deserializing from JSON strings from metastore event
    private static final MessageDeserializer JSON_MESSAGE_DESERIALIZER = new JSONMessageDeserializer();
    // for deserializing from GZIP JSON strings from metastore event
    // (some HDP Hive and CDH Hive versions use this format)
    private static final MessageDeserializer GZIP_JSON_MESSAGE_DESERIALIZER = new GzipJSONMessageDeserializer();
    private static final String GZIP_JSON_FORMAT_PREFIX = "gzip";

    // event factory which is used to get or create MetastoreEvents
    private final MetastoreEventFactory metastoreEventFactory;

    // manager the lastSyncedEventId of hms catalogs
    // use HashMap is fine because all operations are in one thread
    private final Map<Long, Long> lastSyncedEventIdMap = Maps.newHashMap();

    // manager the masterLastSyncedEventId of hms catalogs
    private final Map<Long, Long> masterLastSyncedEventIdMap = Maps.newHashMap();

    private boolean isRunning;

    public MetastoreEventsProcessor() {
        super(MetastoreEventsProcessor.class.getName(), Config.hms_events_polling_interval_ms);
        this.metastoreEventFactory = new MetastoreEventFactory();
        this.isRunning = false;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (isRunning) {
            LOG.warn("Last task not finished,ignore current task.");
            return;
        }
        isRunning = true;
        try {
            realRun();
        } catch (Exception ex) {
            LOG.warn("Task failed", ex);
        }
        isRunning = false;
    }

    private void realRun() {
        List<Long> catalogIds = Env.getCurrentEnv().getCatalogMgr().getCatalogIds();
        for (Long catalogId : catalogIds) {
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
            if (catalog instanceof HMSExternalCatalog) {
                HMSExternalCatalog hmsExternalCatalog = (HMSExternalCatalog) catalog;
                try {
                    List<NotificationEvent> events = getNextHMSEvents(hmsExternalCatalog);
                    if (!events.isEmpty()) {
                        LOG.info("Events size are {} on catalog [{}]", events.size(),
                                hmsExternalCatalog.getName());
                        processEvents(events, hmsExternalCatalog);
                    }
                } catch (MetastoreNotificationFetchException e) {
                    LOG.warn("Failed to fetch hms events on {}. msg: ", hmsExternalCatalog.getName(), e);
                } catch (Exception ex) {
                    LOG.warn("Failed to process hive metastore [{}] events .",
                            hmsExternalCatalog.getName(), ex);
                }
            }
        }
    }

    /**
     * Fetch the next batch of NotificationEvents from metastore. The default batch size is
     * <code>{@link Config#hms_events_batch_size_per_rpc}</code>
     */
    private List<NotificationEvent> getNextHMSEvents(HMSExternalCatalog hmsExternalCatalog) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start to pull events on catalog [{}]", hmsExternalCatalog.getName());
        }
        NotificationEventResponse response;
        if (Env.getCurrentEnv().isMaster()) {
            response = getNextEventResponseForMaster(hmsExternalCatalog);
        } else {
            response = getNextEventResponseForSlave(hmsExternalCatalog);
        }

        if (response == null) {
            return Collections.emptyList();
        }
        return response.getEvents();
    }

    private void doExecute(List<MetastoreEvent> events, HMSExternalCatalog hmsExternalCatalog) {
        for (MetastoreEvent event : events) {
            try {
                event.process();
            } catch (HMSClientException hmsClientException) {
                if (hmsClientException.getCause() != null
                        && hmsClientException.getCause() instanceof NoSuchObjectException) {
                    LOG.warn(event.debugString("Failed to process event and skip"), hmsClientException);
                } else {
                    updateLastSyncedEventId(hmsExternalCatalog, event.getEventId() - 1);
                    throw hmsClientException;
                }
            } catch (Exception e) {
                updateLastSyncedEventId(hmsExternalCatalog, event.getEventId() - 1);
                throw e;
            }
        }
    }

    /**
     * Process the given list of notification events. Useful for tests which provide a list of events
     */
    private void processEvents(List<NotificationEvent> events, HMSExternalCatalog hmsExternalCatalog) {
        //transfer
        List<MetastoreEvent> metastoreEvents = metastoreEventFactory.getMetastoreEvents(events, hmsExternalCatalog);
        doExecute(metastoreEvents, hmsExternalCatalog);
        updateLastSyncedEventId(hmsExternalCatalog, events.get(events.size() - 1).getEventId());
    }

    private NotificationEventResponse getNextEventResponseForMaster(HMSExternalCatalog hmsExternalCatalog)
            throws MetastoreNotificationFetchException {
        long lastSyncedEventId = getLastSyncedEventId(hmsExternalCatalog);
        long currentEventId = getCurrentHmsEventId(hmsExternalCatalog);
        if (lastSyncedEventId < 0) {
            refreshCatalogForMaster(hmsExternalCatalog);
            // invoke getCurrentEventId() and save the event id before refresh catalog to avoid missing events
            // but set lastSyncedEventId to currentEventId only if there is not any problems when refreshing catalog
            updateLastSyncedEventId(hmsExternalCatalog, currentEventId);
            LOG.info(
                    "First pulling events on catalog [{}],refreshCatalog and init lastSyncedEventId,"
                            + "lastSyncedEventId is [{}]",
                    hmsExternalCatalog.getName(), lastSyncedEventId);
            return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Catalog [{}] getNextEventResponse, currentEventId is {}, lastSyncedEventId is {}",
                    hmsExternalCatalog.getName(), currentEventId, lastSyncedEventId);
        }
        if (currentEventId == lastSyncedEventId) {
            LOG.info("Event id not updated when pulling events on catalog [{}]", hmsExternalCatalog.getName());
            return null;
        }

        try {
            return hmsExternalCatalog.getClient().getNextNotification(lastSyncedEventId,
                        Config.hms_events_batch_size_per_rpc, null);
        } catch (MetastoreNotificationFetchException e) {
            // Need a fallback to handle this because this error state can not be recovered until restarting FE
            if (StringUtils.isNotEmpty(e.getMessage())
                    && e.getMessage().contains(HiveMetaStoreClient.REPL_EVENTS_MISSING_IN_METASTORE)) {
                refreshCatalogForMaster(hmsExternalCatalog);
                // set lastSyncedEventId to currentEventId after refresh catalog successfully
                updateLastSyncedEventId(hmsExternalCatalog, currentEventId);
                LOG.warn("Notification events are missing, maybe an event can not be handled "
                        + "or processing rate is too low, fallback to refresh the catalog");
                return null;
            }
            throw e;
        }
    }

    private NotificationEventResponse getNextEventResponseForSlave(HMSExternalCatalog hmsExternalCatalog)
                throws Exception {
        long lastSyncedEventId = getLastSyncedEventId(hmsExternalCatalog);
        long masterLastSyncedEventId = getMasterLastSyncedEventId(hmsExternalCatalog);
        // do nothing if masterLastSyncedEventId has not been synced
        if (masterLastSyncedEventId == -1L) {
            LOG.info("LastSyncedEventId of master has not been synced on catalog [{}]", hmsExternalCatalog.getName());
            return null;
        }
        // do nothing if lastSyncedEventId is equals to masterLastSyncedEventId
        if (lastSyncedEventId == masterLastSyncedEventId) {
            LOG.info("Event id not updated when pulling events on catalog [{}]", hmsExternalCatalog.getName());
            return null;
        }

        if (lastSyncedEventId < 0) {
            refreshCatalogForSlave(hmsExternalCatalog);
            // Use masterLastSyncedEventId to avoid missing events
            updateLastSyncedEventId(hmsExternalCatalog, masterLastSyncedEventId);
            LOG.info(
                    "First pulling events on catalog [{}],refreshCatalog and init lastSyncedEventId,"
                            + "lastSyncedEventId is [{}]",
                    hmsExternalCatalog.getName(), lastSyncedEventId);
            return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Catalog [{}] getNextEventResponse, masterLastSyncedEventId is {}, lastSyncedEventId is {}",
                    hmsExternalCatalog.getName(), masterLastSyncedEventId, lastSyncedEventId);
        }

        // For slave FE nodes, only fetch events which id is lower than masterLastSyncedEventId
        int maxEventSize = Math.min((int) (masterLastSyncedEventId - lastSyncedEventId),
                Config.hms_events_batch_size_per_rpc);
        try {
            return hmsExternalCatalog.getClient().getNextNotification(lastSyncedEventId, maxEventSize, null);
        } catch (MetastoreNotificationFetchException e) {
            // Need a fallback to handle this because this error state can not be recovered until restarting FE
            if (StringUtils.isNotEmpty(e.getMessage())
                    && e.getMessage().contains(HiveMetaStoreClient.REPL_EVENTS_MISSING_IN_METASTORE)) {
                refreshCatalogForSlave(hmsExternalCatalog);
                // set masterLastSyncedEventId to lastSyncedEventId after refresh catalog successfully
                updateLastSyncedEventId(hmsExternalCatalog, masterLastSyncedEventId);
                LOG.warn("Notification events are missing, maybe an event can not be handled "
                        + "or processing rate is too low, fallback to refresh the catalog");
                return null;
            }
            throw e;
        }
    }

    private long getCurrentHmsEventId(HMSExternalCatalog hmsExternalCatalog) {
        CurrentNotificationEventId currentNotificationEventId = hmsExternalCatalog.getClient()
                .getCurrentNotificationEventId();
        if (currentNotificationEventId == null) {
            LOG.warn("Get currentNotificationEventId is null");
            return -1L;
        }
        return currentNotificationEventId.getEventId();
    }

    private long getLastSyncedEventId(HMSExternalCatalog hmsExternalCatalog) {
        // Returns to -1 if not exists, otherwise client.getNextNotification will throw exception
        // Reference to https://github.com/apDdlache/doris/issues/18251
        return lastSyncedEventIdMap.getOrDefault(hmsExternalCatalog.getId(), -1L);
    }

    private void updateLastSyncedEventId(HMSExternalCatalog hmsExternalCatalog, long eventId) {
        lastSyncedEventIdMap.put(hmsExternalCatalog.getId(), eventId);
    }

    private long getMasterLastSyncedEventId(HMSExternalCatalog hmsExternalCatalog) {
        return masterLastSyncedEventIdMap.getOrDefault(hmsExternalCatalog.getId(), -1L);
    }

    public void updateMasterLastSyncedEventId(HMSExternalCatalog hmsExternalCatalog, long eventId) {
        masterLastSyncedEventIdMap.put(hmsExternalCatalog.getId(), eventId);
    }

    private void refreshCatalogForMaster(HMSExternalCatalog hmsExternalCatalog) {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(hmsExternalCatalog.getId());
        log.setInvalidCache(true);
        Env.getCurrentEnv().getCatalogMgr().replayRefreshCatalog(log);
    }

    private void refreshCatalogForSlave(HMSExternalCatalog hmsExternalCatalog) throws Exception {
        // Transfer to master to refresh catalog
        String sql = "REFRESH CATALOG " + hmsExternalCatalog.getName();
        OriginStatement originStmt = new OriginStatement(sql, 0);
        ConnectContext ctx = new ConnectContext();
        ctx.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setEnv(Env.getCurrentEnv());
        MasterOpExecutor masterOpExecutor = new MasterOpExecutor(originStmt, ctx,
                RedirectStatus.FORWARD_WITH_SYNC, false);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Transfer to master to refresh catalog, stmt: {}", sql);
        }
        masterOpExecutor.execute();
    }

    public static MessageDeserializer getMessageDeserializer(String messageFormat) {
        if (messageFormat != null && messageFormat.startsWith(GZIP_JSON_FORMAT_PREFIX)) {
            return GZIP_JSON_MESSAGE_DESERIALIZER;
        }
        return JSON_MESSAGE_DESERIALIZER;
    }
}
