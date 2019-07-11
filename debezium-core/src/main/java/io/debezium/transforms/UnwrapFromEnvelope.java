/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.HashMap;
import java.util.Map;

import io.debezium.config.EnumeratedValue;
import io.debezium.data.Envelope;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Debezium generates CDC (<code>Envelope</code>) records that are struct of values containing values
 * <code>before</code> and <code>after change</code>. Sink connectors usually are not able to work
 * with a complex structure so a user use this SMT to extract <code>after</code> value and send it down
 * unwrapped in <code>Envelope</code>.
 * <p>
 * The functionality is similar to <code>ExtractField</code> SMT but has a special semantics for handling
 * delete events; when delete event is emitted by database then Debezium emits two messages: a delete
 * message and a tombstone message that serves as a signal to Kafka compaction process.
 * <p>
 * The SMT by default drops the tombstone message created by Debezium and converts the delete message into
 * a tombstone message that can be dropped, too, if required.
 * <p>
 * The SMT also has the option to insert fields from the original record's 'source' struct into the new 
 * unwrapped record prefixed with "__" (for example __lsn in Postgres, or __file in MySQL)
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public class UnwrapFromEnvelope<R extends ConnectRecord<R>> implements Transformation<R> {

    final static String DEBEZIUM_OPERATION_HEADER_KEY = "__debezium-operation";
    private static final String PURPOSE = "source field insertion";

    public static enum DeleteHandling implements EnumeratedValue {
        DROP("drop"),
        REWRITE("rewrite"),
        NONE("none");

        private final String value;

        private DeleteHandling(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static DeleteHandling parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (DeleteHandling option : DeleteHandling.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static DeleteHandling parse(String value, String defaultValue) {
            DeleteHandling mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    private static final String ENVELOPE_SCHEMA_NAME_SUFFIX = ".Envelope";
    private static final String DELETED_FIELD = "__deleted!";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final Field DROP_TOMBSTONES = Field.create("drop.tombstones")
            .withDisplayName("Drop tombstones")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("Debezium by default generates a tombstone record to enable Kafka compaction after "
                    + "a delete record was generated. This record is usually filtered out to avoid duplicates "
                    + "as a delete record is converted to a tombstone record, too");

    private static final Field DROP_DELETES = Field.create("drop.deletes")
            .withDisplayName("Drop outgoing tombstones")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Drop delete records converted to tombstones records if a processing connector "
                    + "cannot process them or a compaction is undesirable.");

    private static final Field HANDLE_DELETES = Field.create("delete.handling.mode")
            .withDisplayName("Handle delete records")
            .withEnum(DeleteHandling.class, DeleteHandling.DROP)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How to handle delete records. Options are: "
                    + "none - records are passed,"
                    + "drop - records are removed,"
                    + "rewrite - __deleted field is added to records.");

    private static final Field OPERATION_HEADER = Field.create("operation.header")
            .withDisplayName("Adds the debezium operation into the message header")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Adds the operation {@link FieldName#OPERATION operation} as a header." +
                    "Its key is '" + DEBEZIUM_OPERATION_HEADER_KEY +"'");

    private static final Field ADD_SOURCE_FIELDS = Field.create("add.source.fields")
            .withDisplayName("Adds the specified fields from the 'source' field from the payload if they exist.")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("")
            .withDescription("Adds each field listed from the 'source' element of the payload, prefixed with __ "
                    + "Example: 'version,connector' would add __version and __connector fields");

    private boolean dropTombstones;
    private boolean dropDeletes;
    private DeleteHandling handleDeletes;
    private boolean addOperationHeader;
    private String[] addSourceFields;
    private final ExtractField<R> afterDelegate = new ExtractField.Value<R>();
    private final ExtractField<R> beforeDelegate = new ExtractField.Value<R>();
    private final InsertField<R> removedDelegate = new InsertField.Value<R>();
    private final InsertField<R> updatedDelegate = new InsertField.Value<R>();

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        final Field.Set configFields = Field.setOf(DROP_TOMBSTONES, DROP_DELETES, HANDLE_DELETES);
        if (!config.validateAndRecord(configFields, logger::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        dropTombstones = config.getBoolean(DROP_TOMBSTONES);
        handleDeletes = DeleteHandling.parse(config.getString(HANDLE_DELETES));
        if (config.hasKey(DROP_DELETES.name())) {
            logger.warn("{} option is deprecated. Please use {}", DROP_DELETES.name(), HANDLE_DELETES.name());
            dropDeletes = config.getBoolean(DROP_DELETES);
            if (dropDeletes) {
                handleDeletes = DeleteHandling.DROP;
            } else {
                handleDeletes = DeleteHandling.NONE;
            }
        }

        addOperationHeader = config.getBoolean(OPERATION_HEADER);
        addSourceFields = config.getString(ADD_SOURCE_FIELDS).isEmpty() ?
            new String[0] :  config.getString(ADD_SOURCE_FIELDS).split(",");

        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", "before");
        beforeDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("field", "after");
        afterDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", DELETED_FIELD);
        delegateConfig.put("static.value", "true");
        removedDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", DELETED_FIELD);
        delegateConfig.put("static.value", "false");
        updatedDelegate.configure(delegateConfig);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(final R record) {
        Envelope.Operation operation;
        if (record.value() == null) {
            if (dropTombstones) {
                logger.trace("Tombstone {} arrived and requested to be dropped", record.key());
                return null;
            }
            operation = Envelope.Operation.DELETE;
            if (addOperationHeader) {
                record.headers().addString(DEBEZIUM_OPERATION_HEADER_KEY, operation.toString());
            }
            return record;
        }

        if (addOperationHeader) {
            String operationString = ((Struct) record.value()).getString("op");
            operation = Envelope.Operation.forCode(operationString);

            if (operationString.isEmpty() || operation == null) {
                logger.warn("Unknown operation thus unable to add the operation header into the message");
            } else {
                record.headers().addString(DEBEZIUM_OPERATION_HEADER_KEY, operation.code());
            }
        }

        if (record.valueSchema() == null ||
                record.valueSchema().name() == null ||
                !record.valueSchema().name().endsWith(ENVELOPE_SCHEMA_NAME_SUFFIX)) {
            logger.warn("Expected Envelope for transformation, passing it unchanged");
            return record;
        }
        R newRecord = afterDelegate.apply(record);
        if (newRecord.value() == null) {
            // Handling delete records
            switch (handleDeletes) {
                case DROP:
                    logger.trace("Delete message {} requested to be dropped", record.key());
                    return null;
                case REWRITE:
                    logger.trace("Delete message {} requested to be rewritten", record.key());
                    final R oldRecord = beforeDelegate.apply(record);
                    return removedDelegate.apply(oldRecord);
                default:
                    return newRecord;
            }
        } else {
            // Add on any source fields from the original record to the new unwrapped record
            newRecord = addSourceFields(addSourceFields, record, newRecord);

            // Handling insert and update records
            switch (handleDeletes) {
                case REWRITE:
                    logger.trace("Insert/update message {} requested to be rewritten", record.key());
                    return updatedDelegate.apply(newRecord);
                default:
                    return newRecord;
            }
        }
    }

    private R addSourceFields(String[] addSourceFields, R originalRecord, R unwrappedRecord) {
        // Return unwrappedRecord if no source fields to add
        if(addSourceFields.length == 0) {
            return unwrappedRecord;
        }
        
        final Struct value = requireStruct(unwrappedRecord.value(), PURPOSE);
        Struct source = ((Struct) originalRecord.value()).getStruct("source");
        
        // Get the updated schema from the cache, or create and cache if it doesn't exist
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema(), addSourceFields);
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }   

        // Create the updated struct
        final Struct updatedValue = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        for(String sourceField : addSourceFields) {
            String fieldValue = source.schema().field(sourceField) == null ? "" : source.get(sourceField).toString();
            updatedValue.put("__" + sourceField, fieldValue);
        }

        return unwrappedRecord.newRecord(
            unwrappedRecord.topic(), 
            unwrappedRecord.kafkaPartition(), 
            unwrappedRecord.keySchema(), 
            unwrappedRecord.key(), 
            updatedSchema, 
            updatedValue, 
            unwrappedRecord.timestamp());
    }

    private Schema makeUpdatedSchema(Schema schema, String[] addSourceFields) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        // Get fields from original schema
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        // Add the requested source fields
        for(String sourceField: addSourceFields) {
            builder.field("__" + sourceField, SchemaBuilder.string());
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, DROP_TOMBSTONES, DROP_DELETES, HANDLE_DELETES, OPERATION_HEADER);
        return config;
    }

    @Override
    public void close() {
        beforeDelegate.close();
        afterDelegate.close();
        removedDelegate.close();
        updatedDelegate.close();
    }

}
