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
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class PinotTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final Optional<Boolean> scanParallelismEnabled;
    private final Optional<Boolean> isQueryShort;
    private final Optional<PinotQueryGenerator.GeneratedPql> segmentPqlRealtime;
    private final Optional<PinotQueryGenerator.GeneratedPql> segmentPqlOffline;
    private final Optional<PinotQueryGenerator.GeneratedPql> brokerPql;

    public PinotTableHandle(String connectorId, String schemaName, String tableName)
    {
        this(connectorId, schemaName, tableName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public PinotTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("scanParallelismEnabled") Optional<Boolean> scanParallelismEnabled,
            @JsonProperty("isQueryShort") Optional<Boolean> isQueryShort,
            @JsonProperty("segmentPqlRealtime") Optional<PinotQueryGenerator.GeneratedPql> segmentPqlRealtime,
            @JsonProperty("segmentPqlOffline") Optional<PinotQueryGenerator.GeneratedPql> segmentPqlOffline,
            @JsonProperty("brokerPql") Optional<PinotQueryGenerator.GeneratedPql> brokerPql)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.scanParallelismEnabled = scanParallelismEnabled;
        this.isQueryShort = isQueryShort;
        this.segmentPqlRealtime = requireNonNull(segmentPqlRealtime, "plan node");
        this.segmentPqlOffline = requireNonNull(segmentPqlOffline, "plan node");
        this.brokerPql = requireNonNull(brokerPql, "broker pql");
    }

    @JsonProperty
    public Optional<PinotQueryGenerator.GeneratedPql> getSegmentPqlRealtime()
    {
        return segmentPqlRealtime;
    }

    @JsonProperty
    public Optional<Boolean> getScanParallelismEnabled()
    {
        return scanParallelismEnabled;
    }

    @JsonProperty
    public Optional<PinotQueryGenerator.GeneratedPql> getSegmentPqlOffline()
    {
        return segmentPqlOffline;
    }

    @JsonProperty
    public Optional<PinotQueryGenerator.GeneratedPql> getBrokerPql()
    {
        return brokerPql;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    public Optional<Boolean> getIsQueryShort()
    {
        return isQueryShort;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        PinotTableHandle that = (PinotTableHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(scanParallelismEnabled, that.scanParallelismEnabled) &&
                Objects.equals(isQueryShort, that.isQueryShort) &&
                Objects.equals(segmentPqlRealtime, that.segmentPqlRealtime) &&
                Objects.equals(segmentPqlOffline, that.segmentPqlOffline) &&
                Objects.equals(brokerPql, that.brokerPql);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, scanParallelismEnabled, isQueryShort, segmentPqlRealtime, segmentPqlOffline, brokerPql);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("scanParallelismEnabled", scanParallelismEnabled)
                .add("isQueryShort", isQueryShort)
                .add("segmentPqlRealtime", segmentPqlRealtime)
                .add("segmentPqlOffline", segmentPqlOffline)
                .add("brokerPql", brokerPql)
                .toString();
    }
}
