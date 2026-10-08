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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.TableDistribution;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class MaterializedTableDescriptorTest {

    private static final ConfigOption<Integer> PARALLELISM =
            ConfigOptions.key("sink.parallelism").intType().noDefaultValue();

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("k", DataTypes.INT().notNull())
                    .column("cnt", DataTypes.BIGINT())
                    .primaryKey("k")
                    .build();

    @Test
    void emptyDescriptorHasDefaults() {
        MaterializedTableDescriptor descriptor = MaterializedTableDescriptor.newBuilder().build();

        assertThat(descriptor.getSchema()).isEmpty();
        assertThat(descriptor.getOptions()).isEmpty();
        assertThat(descriptor.getDistribution()).isEmpty();
        assertThat(descriptor.getPartitionKeys()).isEmpty();
        assertThat(descriptor.getComment()).isEmpty();
        assertThat(descriptor.getFreshness()).isEmpty();
        assertThat(descriptor.getRefreshStrategy()).isEqualTo(RefreshStrategy.automatic());
    }

    @Test
    void builderSetsAllFields() {
        MaterializedTableDescriptor descriptor = allFieldsDescriptor();

        assertThat(descriptor.getSchema()).contains(SCHEMA);
        assertThat(descriptor.getOptions())
                .containsExactly(entry("connector", "values"), entry("sink.parallelism", "4"));
        assertThat(descriptor.getDistribution())
                .contains(TableDistribution.ofHash(List.of("k"), 2));
        assertThat(descriptor.getPartitionKeys()).containsExactly("k");
        assertThat(descriptor.getComment()).contains("my mt");
        assertThat(descriptor.getFreshness()).contains(IntervalFreshness.ofMinute(1));
        assertThat(descriptor.getRefreshStrategy()).isEqualTo(RefreshStrategy.continuous());

        assertThat(
                        MaterializedTableDescriptor.newBuilder()
                                .fullRefresh()
                                .build()
                                .getRefreshStrategy())
                .isEqualTo(RefreshStrategy.full());
        assertThat(
                        MaterializedTableDescriptor.newBuilder()
                                .refreshStrategy(RefreshStrategy.continuous())
                                .build()
                                .getRefreshStrategy())
                .isEqualTo(RefreshStrategy.continuous());
    }

    @Test
    void everyDistributionFactoryIsAvailable() {
        assertThat(distribution(b -> b.distributedBy("k")))
                .isEqualTo(TableDistribution.ofUnknown(List.of("k"), null));
        assertThat(distribution(b -> b.distributedBy(3, "k")))
                .isEqualTo(TableDistribution.ofUnknown(List.of("k"), 3));
        assertThat(distribution(b -> b.distributedByHash("k")))
                .isEqualTo(TableDistribution.ofHash(List.of("k"), null));
        assertThat(distribution(b -> b.distributedByRange("k")))
                .isEqualTo(TableDistribution.ofRange(List.of("k"), null));
        assertThat(distribution(b -> b.distributedByRange(3, "k")))
                .isEqualTo(TableDistribution.ofRange(List.of("k"), 3));
        assertThat(distribution(b -> b.distributedInto(3)))
                .isEqualTo(TableDistribution.ofUnknown(3));
    }

    @Test
    void nullableSettersClearTheirValue() {
        MaterializedTableDescriptor cleared =
                allFieldsDescriptor().toBuilder()
                        .schema(null)
                        .comment(null)
                        .freshness((IntervalFreshness) null)
                        .build();

        assertThat(cleared.getSchema()).isEmpty();
        assertThat(cleared.getComment()).isEmpty();
        assertThat(cleared.getFreshness()).isEmpty();
    }

    @Test
    void gettersReturnUnmodifiableCollections() {
        MaterializedTableDescriptor descriptor = allFieldsDescriptor();

        assertThatThrownBy(() -> descriptor.getOptions().put("connector", "other"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> descriptor.getPartitionKeys().add("cnt"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void toBuilderRoundTripsAllFieldsAndDescriptorsAreValues() {
        MaterializedTableDescriptor descriptor = allFieldsDescriptor();

        assertThat(descriptor.toBuilder().build()).isEqualTo(descriptor);
        assertThat(descriptor.toBuilder().build()).hasSameHashCodeAs(descriptor);
        assertThat(descriptor.toBuilder().comment("changed").build()).isNotEqualTo(descriptor);
    }

    @Test
    void durationFreshnessIsStoredInItsLargestWholeUnit() {
        assertThat(freshnessOf(Duration.ofHours(1))).isEqualTo(IntervalFreshness.ofHour(1));
        assertThat(freshnessOf(Duration.ofSeconds(120))).isEqualTo(IntervalFreshness.ofMinute(2));
        assertThat(freshnessOf(Duration.ofSeconds(90))).isEqualTo(IntervalFreshness.ofSecond(90));
    }

    @Test
    void invalidDurationFreshnessIsRejected() {
        assertThatThrownBy(() -> MaterializedTableDescriptor.newBuilder().freshness(Duration.ZERO))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Freshness must be positive, but was: PT0S");
        assertThatThrownBy(
                        () ->
                                MaterializedTableDescriptor.newBuilder()
                                        .freshness(Duration.ofSeconds(-5)))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Freshness must be positive, but was: PT-5S");
        assertThatThrownBy(
                        () ->
                                MaterializedTableDescriptor.newBuilder()
                                        .freshness(Duration.ofMillis(1500)))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Freshness must be a whole number of seconds, but was: PT1.5S");
        assertThatThrownBy(
                        () ->
                                MaterializedTableDescriptor.newBuilder()
                                        .freshness(Duration.ofSeconds(Integer.MAX_VALUE + 1L)))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith("Freshness must be at most 2147483647 seconds, but was: ");
    }

    @Test
    void distributionRequiresBucketKeys() {
        assertThatThrownBy(() -> MaterializedTableDescriptor.newBuilder().distributedByHash())
                .isInstanceOf(ValidationException.class)
                .hasMessage("At least one bucket key must be defined for a distribution.");
    }

    @Test
    void nullArgumentsAreRejected() {
        MaterializedTableDescriptor.Builder builder = MaterializedTableDescriptor.newBuilder();
        assertThatThrownBy(() -> builder.option((String) null, "v"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.option(PARALLELISM, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.freshness((Duration) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.refreshStrategy(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void toStringRendersTheDeclaredClausesInDdlOrder() {
        String rendered = allFieldsDescriptor().toString();

        assertThat(rendered)
                .contains("`k` INT NOT NULL")
                .contains("COMMENT 'my mt'")
                .contains("DISTRIBUTED BY HASH(`k`) INTO 2 BUCKETS")
                .contains("PARTITIONED BY (`k`)")
                .contains("'connector' = 'values'")
                .contains("FRESHNESS = INTERVAL '1' MINUTE")
                .contains("REFRESH_MODE = CONTINUOUS")
                .doesNotContain("START_MODE");

        List<Integer> clauseIndexes =
                List.of(
                        rendered.indexOf("`k` INT NOT NULL"),
                        rendered.indexOf("COMMENT"),
                        rendered.indexOf("DISTRIBUTED BY"),
                        rendered.indexOf("PARTITIONED BY"),
                        rendered.indexOf("WITH ("),
                        rendered.indexOf("FRESHNESS"),
                        rendered.indexOf("REFRESH_MODE"));
        assertThat(clauseIndexes).isSorted().doesNotHaveDuplicates();

        assertThat(MaterializedTableDescriptor.newBuilder().build().toString())
                .doesNotContain("REFRESH_MODE");
    }

    // ---------------------------------------------------------------------------------------------

    private static MaterializedTableDescriptor allFieldsDescriptor() {
        return MaterializedTableDescriptor.newBuilder()
                .schema(SCHEMA)
                .option("connector", "values")
                .option(PARALLELISM, 4)
                .distributedByHash(2, "k")
                .partitionedBy("k")
                .comment("my mt")
                .freshness(IntervalFreshness.ofMinute(1))
                .continuousRefresh()
                .build();
    }

    private static TableDistribution distribution(
            UnaryOperator<MaterializedTableDescriptor.Builder> setter) {
        return setter.apply(MaterializedTableDescriptor.newBuilder())
                .build()
                .getDistribution()
                .orElseThrow(AssertionError::new);
    }

    private static IntervalFreshness freshnessOf(Duration duration) {
        return MaterializedTableDescriptor.newBuilder()
                .freshness(duration)
                .build()
                .getFreshness()
                .orElseThrow(AssertionError::new);
    }
}
