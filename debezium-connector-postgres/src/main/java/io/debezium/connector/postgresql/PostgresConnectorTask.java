/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.LoggingContext;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends BaseSourceTask {
 
    private static final String CONTEXT_NAME = "postgres-connector-task";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean running = new AtomicBoolean(false);

    private String taskName = "";
    private String databaseName = "";
    private PostgresTaskContext taskContext;
    private RecordsProducer producer;

    private final PostgresPendingLsnStore pendingLsnStore = new PostgresPendingLsnStore();
    private Long idleLsnTimeoutMillis;
    // The last LSN we actually committed to Postgres
    private Long lastCommittedLsn = -1L;
    // The time the last LSN was committed
    private LocalDateTime lastCommittedLsnTime;

    /**
     * A queue with change events filled by the snapshot and streaming producers, consumed
     * by Kafka Connect via this task.
     */
    private ChangeEventQueue<ChangeEvent> changeEventQueue;

    @Override
    public void start(Configuration config) {
        if (running.get()) {
            // already running
            return;
        }

        PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);
        this.databaseName = connectorConfig.databaseName();
        this.taskName = connectorConfig.getLogicalName();
        this.idleLsnTimeoutMillis = connectorConfig.idleLsnTimeoutMillis();

        TypeRegistry typeRegistry;
        Charset databaseCharset;

        try (final PostgresConnection connection = new PostgresConnection(connectorConfig.jdbcConfig())) {
            typeRegistry = connection.getTypeRegistry();
            databaseCharset = connection.getDatabaseCharset();
        }

        Snapshotter snapshotter = connectorConfig.getSnapshotter();
        if (snapshotter == null) {
            logger.error("Unable to load snapshotter, if using custom snapshot mode, double check your settings");
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode, double check your settings");
        }

        // create the task context and schema...
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig);
        PostgresSchema schema = new PostgresSchema(connectorConfig, typeRegistry, databaseCharset, topicSelector);
        this.taskContext = new PostgresTaskContext(connectorConfig, schema, topicSelector);

        SourceInfo sourceInfo = new SourceInfo(connectorConfig.getLogicalName(), connectorConfig.databaseName());
        Map<String, Object> existingOffset = context.offsetStorageReader().offset(sourceInfo.partition());
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            //Print out the server information
            SlotState slotInfo = null;
            try (PostgresConnection connection = taskContext.createConnection()) {
                if (logger.isInfoEnabled()) {
                    logger.info(connection.serverInfo().toString());
                }
                slotInfo = connection.getReplicationSlotInfo(connectorConfig.slotName(), connectorConfig.plugin().getPostgresPluginName());
            }
            catch (SQLException e) {
                logger.warn("unable to load info of replication slot, debezium will try to create the slot");
            }

            if (existingOffset == null) {
                logger.info("No previous offset found");
                // if we have no initial offset, indicate that to Snapshotter by passing null
                snapshotter.init(connectorConfig, null, slotInfo);
            }
            else {
                logger.info("Found previous offset {}", sourceInfo);
                sourceInfo.load(existingOffset);
                snapshotter.init(connectorConfig, sourceInfo.asOffsetState(), slotInfo);
            }

            createRecordProducer(taskContext, sourceInfo, snapshotter);

            changeEventQueue = new ChangeEventQueue.Builder<ChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

            producer.start(changeEventQueue::enqueue, changeEventQueue::producerFailure);
            running.compareAndSet(false, true);
        }
        finally {
            previousContext.restore();
        }
    }

    private void createRecordProducer(PostgresTaskContext taskContext, SourceInfo sourceInfo, Snapshotter snapshotter) {
        if (snapshotter.shouldSnapshot()) {
            if (snapshotter.shouldStream()) {
                logger.info("Taking a new snapshot of the DB and streaming logical changes once the snapshot is finished...");
                producer = new RecordsSnapshotProducer(taskContext, sourceInfo, snapshotter);
            }
            else {
                logger.info("Taking only a snapshot of the DB without streaming any changes afterwards...");
                producer = new RecordsSnapshotProducer(taskContext, sourceInfo, snapshotter);
            }
        }
        else if (snapshotter.shouldStream()) {
            logger.info("Not attempting to take a snapshot, immediately starting to stream logical changes...");
            producer = new RecordsStreamProducer(taskContext, sourceInfo);
        }
        else {
            throw new ConnectException("Snapshotter neither is snapshotting or streaming, invalid!");
        }
    }

    @Override
    public void commit() throws InterruptedException {
        if (running.get()) {
            Long fullyProcessedLsn = pendingLsnStore.getFullyProcessedLsn();
            // If the LSN hasn't changed, and it's been over the idle timeout since the last change, stop the connector so it can be restarted
            if(fullyProcessedLsn == lastCommittedLsn && Duration.between(lastCommittedLsnTime, LocalDateTime.now()).toMillis() > this.idleLsnTimeoutMillis) {
                logger.error("[LSN_DEBUG] Stopping connector because LSN has not changed in {}ms {}: {}", this.idleLsnTimeoutMillis, taskName, (fullyProcessedLsn != null ? LogSequenceNumber.valueOf(fullyProcessedLsn) : ""));
                this.stop();
            } else if (fullyProcessedLsn != lastCommittedLsn){
                // If it has change, update the lastCommitted values
                lastCommittedLsn = fullyProcessedLsn;
                lastCommittedLsnTime = LocalDateTime.now();
            }

            // Commit the LSN if not null
            if (fullyProcessedLsn != null) {
                logger.info("[LSN_DEBUG] Committing the largest fully processed LSN so far for connector {}: {}", taskName, LogSequenceNumber.valueOf(fullyProcessedLsn));
                producer.commit(fullyProcessedLsn);
            } else {
                logger.info("[LSN_DEBUG] commit called without any new records for connector {}", taskName);
            }
        }
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        pendingLsnStore.recordProcessedLsn(record);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<ChangeEvent> events = changeEventQueue.poll();

        if (events.size() > 0) {
            Long lastLsn = events.get(events.size() - 1).getLastCompletelyProcessedLsn();
            if (lastLsn != null) {
                logger.info("[LSN_DEBUG] {} - Polling {} events, with last event's lsn ending at: {}", this.databaseName, events.size(), LogSequenceNumber.valueOf(lastLsn));
            }
        }

        List<SourceRecord> records = events.stream().map(ChangeEvent::getRecord).collect(Collectors.toList());
        pendingLsnStore.recordPolledLsns(records);

        return records;
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            producer.stop();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }
}
