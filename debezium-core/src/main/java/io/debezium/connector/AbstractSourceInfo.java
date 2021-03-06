/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Common information provided by all connectors in either source field or offsets
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractSourceInfo {
    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
    public static final String SERVER_NAME_KEY = "name";
    public static final String TIMESTAMP_KEY = "ts_ms";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String DATABASE_NAME_KEY = "db";
    public static final String SCHEMA_NAME_KEY = "schema";
    public static final String TABLE_NAME_KEY = "table";
    public static final String COLLECTION_NAME_KEY = "collection";

    private final String version;

    protected static SchemaBuilder schemaBuilder() {
        return SchemaBuilder.struct()
                .field(DEBEZIUM_VERSION_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DEBEZIUM_CONNECTOR_KEY, Schema.OPTIONAL_STRING_SCHEMA);
    }

    protected AbstractSourceInfo(String version) {
        this.version = Objects.requireNonNull(version);
    }

    /**
     * Returns the schema of specific sub-types. Implementations should call
     * {@link #schemaBuilder()} to add all shared fields to their schema.
     */
    protected abstract Schema schema();

    /**
     * Returns the string that identifies the connector relative to the database. Implementations should override
     * this method to specify the connector identifier.
     *
     * @return the connector identifier
     */
    protected abstract String connector();

    protected Struct struct() {
        return new Struct(schema())
                .put(DEBEZIUM_VERSION_KEY, version)
                .put(DEBEZIUM_CONNECTOR_KEY, connector());
    }
}
