package bio.terra.service.tabulardata.google;

import bio.terra.common.Table;
import bio.terra.service.dataset.Dataset;
import org.stringtemplate.v4.ST;

import static bio.terra.common.PdaoConstant.*;

/**
 * Utilities for generating BigQuery SQL statements.
 */
public final class BigQuerySqlGenerator {

    private static final String addRowIdsToStagingTableTemplate =
        "UPDATE `<project>.<dataset>.<stagingTable>` SET " +
            PDAO_ROW_ID_COLUMN + " = GENERATE_UUID() WHERE " +
            PDAO_ROW_ID_COLUMN + " IS NULL";

    /**
     * Generate SQL which will add unique row IDs to every row
     * in a table of primary data staged for ingest.
     */
    public static String addRowIdsToStagingTable(String bqProjectId, Dataset dataset,
                                                 String stagingTableName) {
        ST template = new ST(addRowIdsToStagingTableTemplate);
        template.add("project", bqProjectId);
        template.add("dataset", prefixName(dataset.getName()));
        template.add("stagingTable", stagingTableName);

        return template.render();
    }

    private static final String insertIntoDatasetTableTemplate =
        "INSERT `<project>.<dataset>.<targetTable>` (<columns; separator=\",\">) " +
            "SELECT <columns; separator=\",\"> FROM `<project>.<dataset>.<stagingTable>`";

    /**
     * Generate SQL which will insert all the rows of a staging table
     * into its target table.
     */
    public static String insertIntoDatasetTable(String bqProjectId, Dataset dataset,
                                                Table targetTable, String stagingTableName) {
        ST template = new ST(insertIntoDatasetTableTemplate);
        template.add("project", bqProjectId);
        template.add("dataset", prefixName(dataset.getName()));
        template.add("targetTable", targetTable.getName());
        template.add("stagingTable", stagingTableName);
        template.add("columns", PDAO_ROW_ID_COLUMN);
        targetTable.getColumns().forEach(column -> template.add("columns", column.getName()));

        return template.render();
    }
}
