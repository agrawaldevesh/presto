/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.pinot;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class PinotSessionProperties
{
    static final String NUM_SEGMENTS_PER_SPLIT = "num_segments_per_split";
    private static final String CONNECTION_TIMEOUT = "connection_timeout";
    private static final String SCAN_PARALLELISM_ENABLED = "scan_parallelism_enabled";
    private static final String IGNORE_EMPTY_RESPONSES = "ignore_empty_responses";
    private static final String RETRY_COUNT = "retry_count";
    private static final String USE_PRESTO_DATE_TRUNC = "use_presto_date_trunc";
    private static final String SCAN_PIPELINE_SCAN_LIMIT = "pinot_scan_pipeline_scan_limit";

    private final List<PropertyMetadata<?>> sessionProperties;

    public static int getNumSegmentsPerSplit(ConnectorSession session)
    {
        return session.getProperty(NUM_SEGMENTS_PER_SPLIT, Integer.class);
    }

    public static boolean isScanParallelismEnabled(ConnectorSession session)
    {
        return session.getProperty(SCAN_PARALLELISM_ENABLED, Boolean.class);
    }

    public static Duration getConnectionTimeout(ConnectorSession session)
    {
        return session.getProperty(CONNECTION_TIMEOUT, Duration.class);
    }

    public static boolean isIgnoreEmptyResponses(ConnectorSession session)
    {
        return session.getProperty(IGNORE_EMPTY_RESPONSES, Boolean.class);
    }

    public static int getPinotRetryCount(ConnectorSession session)
    {
        return session.getProperty(RETRY_COUNT, Integer.class);
    }

    public static boolean isUsePrestoDateTrunc(ConnectorSession session)
    {
        return session.getProperty(USE_PRESTO_DATE_TRUNC, Boolean.class);
    }

    public static Optional<Integer> getScanPipelineScanLimit(ConnectorSession session)
    {
        int value = session.getProperty(SCAN_PIPELINE_SCAN_LIMIT, Integer.class);
        return value <= 0 ? Optional.empty() : Optional.of(value);
    }

    @Inject
    public PinotSessionProperties(PinotConfig pinotConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        SCAN_PARALLELISM_ENABLED,
                        "Scan Parallelism enabled",
                        pinotConfig.isScanParallelismEnabled(),
                        false),
                booleanProperty(
                        IGNORE_EMPTY_RESPONSES,
                        "Ignore empty or missing pinot server responses",
                        pinotConfig.isIgnoreEmptyResponses(),
                        false),
                integerProperty(
                        RETRY_COUNT,
                        "Retry count for retriable pinot data fetch calls",
                        pinotConfig.getFetchRetryCount(),
                        false),
                integerProperty(
                        SCAN_PIPELINE_SCAN_LIMIT,
                        "Scan pipeline scan limit",
                        pinotConfig.getScanPipelineScanLimit(),
                        false),
                booleanProperty(
                        USE_PRESTO_DATE_TRUNC,
                        "Use SQL compatible date_trunc",
                        pinotConfig.isUsePrestoDateTrunc(),
                        false),
                new PropertyMetadata<>(
                        CONNECTION_TIMEOUT,
                        "Connection Timeout to talk to Pinot servers",
                        createUnboundedVarcharType(),
                        Duration.class,
                        pinotConfig.getConnectionTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                new PropertyMetadata<>(
                        NUM_SEGMENTS_PER_SPLIT,
                        "Number of segments of the same host per split",
                        INTEGER,
                        Integer.class,
                        pinotConfig.getNumSegmentsPerSplit(),
                        false,
                        value -> {
                            int ret = ((Number) value).intValue();
                            if (ret <= 0) {
                                throw new IllegalArgumentException("Number of segments per split must be more than zero");
                            }
                            return ret;
                        },
                        object -> object));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
