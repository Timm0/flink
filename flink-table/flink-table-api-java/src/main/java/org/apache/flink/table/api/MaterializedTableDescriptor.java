/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Describes a {@link CatalogMaterializedTable} representing a materialized table.
 *
 * <p>A materialized table is a query kept fresh by the engine, so a descriptor is closer to a
 * {@code CREATE MATERIALIZED TABLE} statement than to a table definition: freshness, refresh mode
 * and start mode belong here, while the schema is normally derived from the query.
 *
 * <p>Deliberately <b>not</b> a {@link TableDescriptor}. The two share options but not meaning — a
 * materialized table has no connector, so there is no {@code forConnector()} — and keeping them
 * apart stops one being passed where the other is expected.
 */
@PublicEvolving
public class MaterializedTableDescriptor {

    private final @Nullable Schema schema;
    private final Map<String, String> options;
    private final @Nullable TableDistribution distribution;
    private final List<String> partitionKeys;
    private final @Nullable String comment;
    private final @Nullable IntervalFreshness freshness;
    private final LogicalRefreshMode refreshMode;
    private final @Nullable StartMode startMode;

    protected MaterializedTableDescriptor(
            @Nullable Schema schema,
            Map<String, String> options,
            @Nullable TableDistribution distribution,
            List<String> partitionKeys,
            @Nullable String comment,
            @Nullable IntervalFreshness freshness,
            LogicalRefreshMode refreshMode,
            @Nullable StartMode startMode) {
        this.schema = schema;
        this.options = options;
        this.distribution = distribution;
        this.partitionKeys = partitionKeys;
        this.comment = comment;
        this.freshness = freshness;
        this.refreshMode = refreshMode;
        this.startMode = startMode;
    }

    protected MaterializedTableDescriptor(MaterializedTableDescriptor descriptor) {
        this(
                descriptor.schema,
                descriptor.options,
                descriptor.distribution,
                descriptor.partitionKeys,
                descriptor.comment,
                descriptor.freshness,
                descriptor.refreshMode,
                descriptor.startMode);
    }

    /** Creates a new {@link Builder} with nothing set. */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates a new {@link Builder} with the given freshness.
     *
     * <p>Every materialized table has a freshness, so starting from it reads more like the
     * declaration it becomes.
     */
    public static Builder ofFreshness(Duration freshness) {
        return newBuilder().freshness(freshness);
    }

    /** Converts this immutable instance into a mutable {@link Builder}. */
    public Builder toBuilder() {
        return new Builder(this);
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Returns the declared schema, if any.
     *
     * <p>Usually absent: the schema is derived from the query. Declare one to add a watermark, a
     * primary key, or to pin column order.
     */
    public Optional<Schema> getSchema() {
        return Optional.ofNullable(schema);
    }

    /** Returns a map of string-based options. */
    public Map<String, String> getOptions() {
        return Map.copyOf(options);
    }

    public Optional<TableDistribution> getDistribution() {
        return Optional.ofNullable(distribution);
    }

    public List<String> getPartitionKeys() {
        return List.copyOf(partitionKeys);
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    public Optional<IntervalFreshness> getFreshness() {
        return Optional.ofNullable(freshness);
    }

    /** Returns the declared refresh mode, {@link LogicalRefreshMode#AUTOMATIC} if left to Flink. */
    public LogicalRefreshMode getRefreshMode() {
        return refreshMode;
    }

    public Optional<StartMode> getStartMode() {
        return Optional.ofNullable(startMode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MaterializedTableDescriptor that = (MaterializedTableDescriptor) o;
        return Objects.equals(schema, that.schema)
                && options.equals(that.options)
                && Objects.equals(distribution, that.distribution)
                && partitionKeys.equals(that.partitionKeys)
                && Objects.equals(comment, that.comment)
                && Objects.equals(freshness, that.freshness)
                && refreshMode == that.refreshMode
                && Objects.equals(startMode, that.startMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                options,
                distribution,
                partitionKeys,
                comment,
                freshness,
                refreshMode,
                startMode);
    }

    @Override
    public String toString() {
        return "MaterializedTableDescriptor{"
                + "freshness="
                + freshness
                + ", refreshMode="
                + refreshMode
                + ", startMode="
                + startMode
                + ", partitionKeys="
                + partitionKeys
                + ", options="
                + options
                + ", comment="
                + comment
                + '}';
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link MaterializedTableDescriptor}. */
    @PublicEvolving
    public static class Builder {

        private @Nullable Schema schema;
        private final Map<String, String> options;
        private @Nullable TableDistribution distribution;
        private final List<String> partitionKeys;
        private @Nullable String comment;
        private @Nullable IntervalFreshness freshness;
        private LogicalRefreshMode refreshMode;
        private @Nullable StartMode startMode;

        protected Builder() {
            this.options = new HashMap<>();
            this.partitionKeys = new ArrayList<>();
            this.refreshMode = LogicalRefreshMode.AUTOMATIC;
        }

        protected Builder(MaterializedTableDescriptor descriptor) {
            this.schema = descriptor.schema;
            this.options = new LinkedHashMap<>(descriptor.options);
            this.distribution = descriptor.distribution;
            this.partitionKeys = new ArrayList<>(descriptor.partitionKeys);
            this.comment = descriptor.comment;
            this.freshness = descriptor.freshness;
            this.refreshMode = descriptor.refreshMode;
            this.startMode = descriptor.startMode;
        }

        /**
         * Declares the schema explicitly, on top of what the query already defines.
         *
         * <p>Leave it unset to take the query's schema as-is. Declare one to add a watermark or
         * primary key, or to pin column order.
         */
        public Builder schema(@Nullable Schema schema) {
            this.schema = schema;
            return this;
        }

        /** Sets the given option on the materialized table. */
        public <T> Builder option(ConfigOption<T> configOption, T value) {
            Preconditions.checkNotNull(configOption, "Config option must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            options.put(configOption.key(), ConfigurationUtils.convertValue(value, String.class));
            return this;
        }

        /** Sets the given option on the materialized table. Keys must be fully specified. */
        public Builder option(String key, String value) {
            Preconditions.checkNotNull(key, "Key must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            options.put(key, value);
            return this;
        }

        /**
         * Defines that the table should be distributed into buckets over the given columns. The
         * number of buckets and used algorithm are connector-defined.
         */
        public Builder distributedBy(String... bucketKeys) {
            validateBucketKeys(bucketKeys);
            this.distribution = TableDistribution.ofUnknown(Arrays.asList(bucketKeys), null);
            return this;
        }

        /**
         * Defines that the table should be distributed into the given number of buckets by the
         * given columns. The used algorithm is connector-defined.
         */
        public Builder distributedBy(int numberOfBuckets, String... bucketKeys) {
            validateBucketKeys(bucketKeys);
            this.distribution =
                    TableDistribution.ofUnknown(Arrays.asList(bucketKeys), numberOfBuckets);
            return this;
        }

        /**
         * Defines that the table should be distributed into buckets using a hash algorithm over the
         * given columns. The number of buckets is connector-defined.
         */
        public Builder distributedByHash(String... bucketKeys) {
            validateBucketKeys(bucketKeys);
            this.distribution = TableDistribution.ofHash(Arrays.asList(bucketKeys), null);
            return this;
        }

        /**
         * Defines that the table should be distributed into the given number of buckets using a
         * hash algorithm over the given columns.
         */
        public Builder distributedByHash(int numberOfBuckets, String... bucketKeys) {
            validateBucketKeys(bucketKeys);
            this.distribution =
                    TableDistribution.ofHash(Arrays.asList(bucketKeys), numberOfBuckets);
            return this;
        }

        /**
         * Defines that the table should be distributed into buckets using a range algorithm over
         * the given columns. The number of buckets is connector-defined.
         */
        public Builder distributedByRange(String... bucketKeys) {
            validateBucketKeys(bucketKeys);
            this.distribution = TableDistribution.ofRange(Arrays.asList(bucketKeys), null);
            return this;
        }

        /**
         * Defines that the table should be distributed into the given number of buckets using a
         * range algorithm over the given columns.
         */
        public Builder distributedByRange(int numberOfBuckets, String... bucketKeys) {
            validateBucketKeys(bucketKeys);
            this.distribution =
                    TableDistribution.ofRange(Arrays.asList(bucketKeys), numberOfBuckets);
            return this;
        }

        /**
         * Defines that the table should be distributed into the given number of buckets. The
         * algorithm is connector-defined.
         */
        public Builder distributedInto(int numberOfBuckets) {
            this.distribution = TableDistribution.ofUnknown(numberOfBuckets);
            return this;
        }

        /** Defines which columns this materialized table is partitioned by. */
        public Builder partitionedBy(String... partitionKeys) {
            this.partitionKeys.addAll(Arrays.asList(partitionKeys));
            return this;
        }

        /** Defines the comment for this materialized table. */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /** Defines how far behind the source the materialized table is allowed to fall. */
        public Builder freshness(@Nullable IntervalFreshness freshness) {
            this.freshness = freshness;
            return this;
        }

        /**
         * Defines how far behind the source the materialized table is allowed to fall.
         *
         * <p>The duration is mapped onto the coarsest unit that represents it exactly, so {@code
         * Duration.ofSeconds(120)} becomes {@code INTERVAL '2' MINUTE}.
         */
        public Builder freshness(Duration freshness) {
            Preconditions.checkNotNull(freshness, "Freshness must not be null.");
            return freshness(IntervalFreshness.fromDuration(freshness));
        }

        /**
         * Defines whether the table is refreshed by a continuously running job or by periodic batch
         * jobs. Defaults to {@link LogicalRefreshMode#AUTOMATIC}, which lets Flink decide from the
         * freshness.
         */
        public Builder refreshMode(@Nullable LogicalRefreshMode refreshMode) {
            this.refreshMode = refreshMode == null ? LogicalRefreshMode.AUTOMATIC : refreshMode;
            return this;
        }

        /** Defines where the first refresh of this materialized table starts reading. */
        public Builder startMode(@Nullable StartMode startMode) {
            this.startMode = startMode;
            return this;
        }

        /** Returns an immutable instance of {@link MaterializedTableDescriptor}. */
        public MaterializedTableDescriptor build() {
            return new MaterializedTableDescriptor(
                    schema,
                    options,
                    distribution,
                    partitionKeys,
                    comment,
                    freshness,
                    refreshMode,
                    startMode);
        }

        private static void validateBucketKeys(String[] bucketKeys) {
            Preconditions.checkNotNull(bucketKeys, "Bucket keys must not be null.");
            if (bucketKeys.length == 0) {
                throw new ValidationException(
                        "At least one bucket key must be defined for a distribution.");
            }
        }
    }
}
