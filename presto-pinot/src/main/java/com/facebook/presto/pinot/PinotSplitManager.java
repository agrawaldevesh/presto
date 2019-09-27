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

import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.Iterables;
import com.linkedin.pinot.client.PinotClientException;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotSplit.createBrokerSplit;
import static com.facebook.presto.pinot.PinotSplit.createSegmentSplit;
import static com.facebook.presto.pinot.PinotUtils.checkType;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class PinotSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PinotSplitManager.class);
    private final String connectorId;
    private final PinotConnection pinotPrestoConnection;

    @Inject
    public PinotSplitManager(PinotConnectorId connectorId, PinotConnection pinotPrestoConnection)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotPrestoConnection = requireNonNull(pinotPrestoConnection, "pinotPrestoConnection is null");
    }

    protected ConnectorSplitSource generateSplitForBrokerBasedScan(PinotQueryGenerator.GeneratedPql brokerPql)
    {
        return new FixedSplitSource(singletonList(createBrokerSplit(connectorId, brokerPql)));
    }

    protected ConnectorSplitSource generateSplitsForSegmentBasedScan(PinotTableLayoutHandle pinotLayoutHandle, ConnectorSession session)
    {
        PinotTableHandle tableHandle = pinotLayoutHandle.getTable();
        String tableName = tableHandle.getTableName();
        Map<String, Map<String, List<String>>> routingTable;

        try {
            routingTable = pinotPrestoConnection.getRoutingTable(tableName);
        }
        catch (Exception e) {
            log.error("Failed to fetch table status for Pinot table: %s, Exceptions: %s", tableName, e);
            throw new PinotClientException("Failed to fetch table status for Pinot table: " + tableName, e);
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        if (!routingTable.isEmpty()) {
            generateSegmentSplits(splits, routingTable, tableName, "_REALTIME", session, tableHandle.getSegmentPqlRealtime().get().getPql());
            generateSegmentSplits(splits, routingTable, tableName, "_OFFLINE", session, tableHandle.getSegmentPqlOffline().get().getPql());
        }

        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }

    protected void generateSegmentSplits(List<ConnectorSplit> splits, Map<String, Map<String, List<String>>> routingTable,
            String tableName, String tableNameSuffix, ConnectorSession session, String pql)
    {
        final String finalTableName = tableName + tableNameSuffix;
        for (String routingTableName : routingTable.keySet()) {
            if (!routingTableName.equalsIgnoreCase(finalTableName)) {
                continue;
            }

            Map<String, List<String>> hostToSegmentsMap = routingTable.get(routingTableName);
            hostToSegmentsMap.forEach((host, segments) -> {
                // segments is already shuffled
                Iterables.partition(segments, Math.min(segments.size(), PinotSessionProperties.getNumSegmentsPerSplit(session))).forEach(segmentsForThisSplit -> splits.add(createSegmentSplit(connectorId, pql, segmentsForThisSplit, host)));
            });
        }
    }

    public enum QueryNotAdequatelyPushedDownErrorCode
            implements ErrorCodeSupplier
    {
        SCAN_PIPELINE_NOT_SHORT(1, USER_ERROR, "Query uses unsupported expressions that cannot be pushed into the storage engine. Please see https://XXX for more details");

        private final ErrorCode errorCode;

        QueryNotAdequatelyPushedDownErrorCode(int code, ErrorType type, String guidance)
        {
            errorCode = new ErrorCode(code + 0x0625_0000, name() + ": " + guidance, type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }
    }

    public static class QueryNotAdequatelyPushedDownException
            extends PrestoException
    {
        private final String connectorId;
        private final ConnectorTableHandle connectorTableHandle;

        public QueryNotAdequatelyPushedDownException(QueryNotAdequatelyPushedDownErrorCode errorCode, ConnectorTableHandle connectorTableHandle, String connectorId)
        {
            super(errorCode, (String) null);
            this.connectorId = connectorId;
            this.connectorTableHandle = connectorTableHandle;
        }

        @Override
        public String getMessage()
        {
            StringBuilder ret = new StringBuilder(super.getMessage());
            ret.append(String.format(" table: %s:%s", connectorId, connectorTableHandle));
            return ret.toString();
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        PinotTableLayoutHandle pinotLayoutHandle = checkType(layout, PinotTableLayoutHandle.class, "expected a Pinot table layout handle");
        PinotTableHandle pinotTableHandle = pinotLayoutHandle.getTable();
        boolean scanParallelismEnabled = pinotTableHandle.getScanParallelismEnabled().orElseThrow(() -> new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected scan parallelism"));
        if (scanParallelismEnabled) {
            return generateSplitsForSegmentBasedScan(pinotLayoutHandle, session);
        }
        else {
            if (pinotTableHandle.getIsQueryShort().orElse(false)) {
                return generateSplitForBrokerBasedScan(pinotTableHandle.getBrokerPql().orElseThrow(() -> new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), String.format("Couldn't find the PQL embedded into the layout %s", pinotLayoutHandle))));
            }
            else {
                throw new QueryNotAdequatelyPushedDownException(QueryNotAdequatelyPushedDownErrorCode.SCAN_PIPELINE_NOT_SHORT, pinotTableHandle, connectorId);
            }
        }
    }
}
