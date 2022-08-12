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
package io.trino.tests.product.iceberg;

import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.ICEBERG_FORMAT_VERSION_COMPATIBILITY;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onCompatibilityTestServer;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestIcebergFormatVersionCompatibility
        extends ProductTest
{
    private static final String TRINO_CATALOG = "iceberg";
    private static final String TEST_SCHEMA_NAME = "default";

    @Test(groups = {ICEBERG_FORMAT_VERSION_COMPATIBILITY, PROFILE_SPECIFIC_TESTS})
    public void testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino()
    {
        String baseTableName = "test_trino_reading_primitive_types_" + randomTableSuffix();
        String tableName = trinoTableName(baseTableName);

        onCompatibilityTestServer().executeQuery(format("CREATE TABLE %s (c VARCHAR)", tableName));
        onCompatibilityTestServer().executeQuery(format("INSERT INTO %s VALUES ('a'), ('b'), ('c');", tableName));

        long latestSnapshotId = (long) onCompatibilityTestServer()
                .executeQuery(format("SELECT snapshot_id FROM %s ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", trinoTableName("\"" + baseTableName + "$snapshots\"")))
                .row(0).get(0);
        assertThat(onTrino().executeQuery(format("SELECT snapshot_id FROM %s ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", trinoTableName("\"" + baseTableName + "$snapshots\""))))
                .containsOnly(row(latestSnapshotId));

        List<QueryAssert.Row> expected = onCompatibilityTestServer().executeQuery(format("SELECT * FROM %s", tableName)).rows().stream()
                .map(row -> row(row.toArray()))
                .collect(toImmutableList());
        assertEquals(expected.size(), 3);
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s FOR VERSION AS OF %d", tableName, latestSnapshotId))).containsOnly(expected);

        onCompatibilityTestServer().executeQuery(format("DROP TABLE %s", tableName));
    }

    private static String trinoTableName(String tableName)
    {
        return format("%s.%s.%s", TRINO_CATALOG, TEST_SCHEMA_NAME, tableName);
    }
}
