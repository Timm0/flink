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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/** The start mode of materialized table. */
@PublicEvolving
public class StartMode {
    private final StartModeKind kind;
    private final @Nullable Instant timestamp;
    private final boolean localTimeZone;
    private final @Nullable Interval interval;

    @PublicEvolving
    public enum StartModeKind {
        FROM_BEGINNING,
        FROM_NOW,
        FROM_TIMESTAMP,
        RESUME_OR_FROM_BEGINNING,
        RESUME_OR_FROM_NOW,
        RESUME_OR_FROM_TIMESTAMP;
    }

    private StartMode(
            StartModeKind kind,
            @Nullable Instant timestamp,
            boolean localTimeZone,
            @Nullable Interval interval) {
        this.kind = kind;
        this.timestamp = timestamp;
        this.localTimeZone = localTimeZone;
        this.interval = interval;
    }

    public static StartMode of(StartModeKind kind) {
        return new StartMode(kind, null, false, null);
    }

    public static StartMode of(StartModeKind kind, Instant timestamp) {
        return new StartMode(kind, timestamp, false, null);
    }

    public static StartMode of(StartModeKind kind, Instant timestamp, boolean localTimeZone) {
        return new StartMode(kind, timestamp, localTimeZone, null);
    }

    public static StartMode of(StartModeKind kind, Interval interval) {
        return new StartMode(kind, null, false, interval);
    }

    /** Equivalent to {@code START_MODE = FROM_BEGINNING}. */
    public static StartMode fromBeginning() {
        return of(StartModeKind.FROM_BEGINNING);
    }

    /** Equivalent to {@code START_MODE = FROM_NOW}. */
    public static StartMode fromNow() {
        return of(StartModeKind.FROM_NOW);
    }

    /**
     * Equivalent to {@code START_MODE = FROM_NOW(INTERVAL ...)}, which declares a start position of
     * now minus the offset.
     *
     * <p>The interval uses the largest unit among DAY, HOUR, MINUTE and SECOND that divides the
     * offset exactly. For example, {@code Duration.ofMinutes(60)} becomes {@code INTERVAL '1'
     * HOUR}. Use {@link #of(StartModeKind, Interval)} for WEEK, MONTH, QUARTER or YEAR.
     *
     * @throws ValidationException if the offset is not positive, is not a whole number of seconds,
     *     or its amount in that unit exceeds {@link Integer#MAX_VALUE}
     */
    public static StartMode fromNow(Duration offset) {
        return of(StartModeKind.FROM_NOW, toOffsetInterval(offset));
    }

    /**
     * Equivalent to {@code START_MODE = FROM_TIMESTAMP(TIMESTAMP '...')}, where the literal is the
     * instant's UTC date-time, independent of the session time zone. For example, {@code
     * fromTimestamp(Instant.parse("2025-01-15T10:00:00Z"))} equals {@code FROM_TIMESTAMP(TIMESTAMP
     * '2025-01-15 10:00:00')}.
     */
    public static StartMode fromTimestamp(Instant timestamp) {
        return of(StartModeKind.FROM_TIMESTAMP, toMillisecondPrecision(timestamp), false);
    }

    /** Equivalent to {@code START_MODE = RESUME_OR_FROM_BEGINNING}. */
    public static StartMode resumeOrFromBeginning() {
        return of(StartModeKind.RESUME_OR_FROM_BEGINNING);
    }

    /** Equivalent to {@code START_MODE = RESUME_OR_FROM_NOW}. */
    public static StartMode resumeOrFromNow() {
        return of(StartModeKind.RESUME_OR_FROM_NOW);
    }

    /**
     * Equivalent to {@code START_MODE = RESUME_OR_FROM_NOW(INTERVAL ...)}, with the offset handled
     * as in {@link #fromNow(Duration)}.
     *
     * @throws ValidationException under the same conditions as {@link #fromNow(Duration)}
     */
    public static StartMode resumeOrFromNow(Duration offset) {
        return of(StartModeKind.RESUME_OR_FROM_NOW, toOffsetInterval(offset));
    }

    /**
     * Equivalent to {@code START_MODE = RESUME_OR_FROM_TIMESTAMP(TIMESTAMP '...')}, with the
     * timestamp handled as in {@link #fromTimestamp(Instant)}.
     */
    public static StartMode resumeOrFromTimestamp(Instant timestamp) {
        return of(StartModeKind.RESUME_OR_FROM_TIMESTAMP, toMillisecondPrecision(timestamp), false);
    }

    public static boolean requiresParameters(StartModeKind kind) {
        return kind == StartModeKind.FROM_TIMESTAMP
                || kind == StartModeKind.RESUME_OR_FROM_TIMESTAMP;
    }

    private static Instant toMillisecondPrecision(Instant timestamp) {
        Preconditions.checkNotNull(timestamp, "Start mode timestamp must not be null.");
        return timestamp.truncatedTo(ChronoUnit.MILLIS);
    }

    private static Interval toOffsetInterval(Duration offset) {
        Preconditions.checkNotNull(offset, "Start mode offset must not be null.");
        if (offset.isNegative() || offset.isZero()) {
            throw new ValidationException(
                    String.format("The start mode offset must be positive, but was: %s.", offset));
        }
        if (offset.getNano() != 0) {
            throw new ValidationException(
                    String.format(
                            "The start mode offset must be a whole number of seconds, but was: %s.",
                            offset));
        }

        if (offset.equals(offset.truncatedTo(ChronoUnit.DAYS))) {
            return checkedInterval(offset, offset.toDays(), Interval.TimeUnit.DAY);
        }
        if (offset.equals(offset.truncatedTo(ChronoUnit.HOURS))) {
            return checkedInterval(offset, offset.toHours(), Interval.TimeUnit.HOUR);
        }
        if (offset.equals(offset.truncatedTo(ChronoUnit.MINUTES))) {
            return checkedInterval(offset, offset.toMinutes(), Interval.TimeUnit.MINUTE);
        }

        return checkedInterval(offset, offset.getSeconds(), Interval.TimeUnit.SECOND);
    }

    private static Interval checkedInterval(
            Duration offset, long amount, Interval.TimeUnit timeUnit) {
        if (amount > Integer.MAX_VALUE) {
            throw new ValidationException(
                    String.format(
                            "The start mode offset %s is too large, it must be at most %d %s, "
                                    + "the largest unit that divides it exactly.",
                            offset, Integer.MAX_VALUE, timeUnit));
        }

        return Interval.of(offset, timeUnit);
    }

    public StartModeKind getKind() {
        return kind;
    }

    @Nullable
    public Instant getTimestamp() {
        return timestamp;
    }

    public boolean isLocalTimeZone() {
        return localTimeZone;
    }

    @Nullable
    public Interval getInterval() {
        return interval;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartMode startMode = (StartMode) o;
        return localTimeZone == startMode.localTimeZone
                && kind == startMode.kind
                && Objects.equals(timestamp, startMode.timestamp)
                && Objects.equals(interval, startMode.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, timestamp, localTimeZone, interval);
    }

    public String asSummaryString() {
        switch (kind) {
            case FROM_BEGINNING:
            case RESUME_OR_FROM_BEGINNING:
                return kind.name();
            case FROM_NOW:
            case RESUME_OR_FROM_NOW:
                if (interval == null) {
                    return kind.name();
                }

                return kind.name() + "(" + interval + ")";

            case FROM_TIMESTAMP:
            case RESUME_OR_FROM_TIMESTAMP:
                return kind.name()
                        + "(TIMESTAMP "
                        + (localTimeZone ? "WITH LOCAL TIME ZONE " : "")
                        + "'"
                        + timestamp
                        + "')";

            default:
                throw new IllegalStateException("Unexpected StartModeKind: " + kind);
        }
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
