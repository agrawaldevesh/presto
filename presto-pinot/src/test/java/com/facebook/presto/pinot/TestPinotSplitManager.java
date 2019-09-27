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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.testing.TestingConnectorContext;

import java.util.concurrent.Executors;

import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.ColorType.COLOR;

public class TestPinotSplitManager
{
    // Test table and related info
    private static final PinotColumnHandle columnCityId = new PinotColumnHandle("city_id", BIGINT, REGULAR);
    private static final PinotColumnHandle columnCountryName = new PinotColumnHandle("country_name", VARCHAR, REGULAR);
    private static final PinotColumnHandle columnColor = new PinotColumnHandle("color", COLOR, REGULAR);

    public static PinotTableHandle realtimeOnlyTable = new PinotTableHandle("connId", "schema", "realtimeOnly");
    public static PinotTableHandle hybridTable = new PinotTableHandle("connId", "schema", "hybrid");
    public static ColumnHandle regionId = new PinotColumnHandle("regionId", BIGINT, REGULAR);
    public static ColumnHandle city = new PinotColumnHandle("city", VARCHAR, REGULAR);
    public static ColumnHandle fare = new PinotColumnHandle("fare", DOUBLE, REGULAR);
    public static ColumnHandle secondsSinceEpoch = new PinotColumnHandle("secondsSinceEpoch", BIGINT, REGULAR);

    private final PinotConfig pinotConfig = new PinotConfig();
    private final PinotMetadata pinotMetadata;
    private final ConnectorContext context = new TestingConnectorContext();
    private final PinotConnectorId pinotConnectorId = new PinotConnectorId("id");
    private final PinotSplitManager pinotSplitManager;
    private final PinotConnection pinotConnection = new PinotConnection(new MockPinotClusterInfoFetcher(pinotConfig), pinotConfig, Executors.newSingleThreadExecutor());

    public TestPinotSplitManager()
    {
        pinotMetadata = new PinotMetadata(pinotConnectorId, pinotConnection);
        pinotSplitManager = new PinotSplitManager(pinotConnectorId, pinotConnection);
    }
}
