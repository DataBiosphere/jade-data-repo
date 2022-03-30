package bio.terra.service.tabulardata.google.bigquery;

import static bio.terra.common.PdaoConstant.PDAO_EXTERNAL_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_FIRESTORE_DUMP_FILE_ID_KEY;
import static bio.terra.common.PdaoConstant.PDAO_FIRESTORE_DUMP_GSPATH_KEY;
import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;

import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.service.common.gcs.BigQueryUtils;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
@Profile("google")
public class BigQueryPdao {

  static void grantReadAccessWorker(
      BigQueryProject bigQueryProject, String name, Collection<String> policyGroupEmails)
      throws InterruptedException {
    List<Acl> policyGroupAcls =
        policyGroupEmails.stream()
            .map(email -> Acl.of(new Acl.Group(email), Acl.Role.READER))
            .collect(Collectors.toList());
    bigQueryProject.addDatasetAcls(name, policyGroupAcls);
  }

  private static final String selectHasDuplicateStagingIdsTemplate =
      "SELECT <pkColumns:{c|<c.name>}; separator=\",\">,COUNT(*) "
          + "FROM `<project>.<dataset>.<tableName>` "
          + "GROUP BY <pkColumns:{c|<c.name>}; separator=\",\"> "
          + "HAVING COUNT(*) > 1";

  /**
   * Returns true is any duplicate IDs are present in a BigQuery table TODO: add support for
   * returning top few instances
   */
  public boolean hasDuplicatePrimaryKeys(
      FSContainerInterface container, List<Column> pkColumns, String tableName)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(container);

    String bqDatasetName = prefixContainerName(container);

    ST sqlTemplate = new ST(selectHasDuplicateStagingIdsTemplate);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", bqDatasetName);
    sqlTemplate.add("tableName", tableName);
    sqlTemplate.add("pkColumns", pkColumns);

    TableResult result = bigQueryProject.query(sqlTemplate.render());
    return result.getTotalRows() > 0;
  }

  public static String prefixName(String name) {
    return PDAO_PREFIX + name;
  }

  static String externalTableName(String tableName, String suffix) {
    return String.format("%s%s_%s", PDAO_EXTERNAL_TABLE_PREFIX, tableName, suffix);
  }

  public void createFirestoreGsPathExternalTable(Snapshot snapshot, String path, String flightId) {
    String gsPathMappingTableName = BigQueryUtils.gsPathMappingTableName(snapshot);
    String extTableName = externalTableName(gsPathMappingTableName, flightId);

    BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);
    TableId tableId = TableId.of(snapshot.getName(), extTableName);
    Schema schema =
        Schema.of(
            Field.of(PDAO_FIRESTORE_DUMP_FILE_ID_KEY, LegacySQLTypeName.STRING),
            Field.of(PDAO_FIRESTORE_DUMP_GSPATH_KEY, LegacySQLTypeName.STRING));
    ExternalTableDefinition tableDef =
        ExternalTableDefinition.of(path, schema, FormatOptions.json());
    TableInfo tableInfo = TableInfo.of(tableId, tableDef);
    bigQueryProject.getBigQuery().create(tableInfo);
  }

  public boolean deleteFirestoreGsPathExternalTable(Snapshot snapshot, String flightId) {
    String gsPathMappingTableName = BigQueryUtils.gsPathMappingTableName(snapshot);
    return deleteExternalTable(snapshot, gsPathMappingTableName, flightId);
  }

  public boolean deleteExternalTable(
      FSContainerInterface container, String tableName, String suffix) {

    BigQueryProject bigQueryProject = BigQueryProject.from(container);
    String bqDatasetName = prefixContainerName(container);
    String extTableName = externalTableName(tableName, suffix);
    return bigQueryProject.deleteTable(bqDatasetName, extTableName);
  }

  private String prefixContainerName(FSContainerInterface container) {
    if (container.getCollectionType() == CollectionType.DATASET) {
      return prefixName(container.getName());
    } else {
      return container.getName();
    }
  }

  private static final String exportToParquetTemplate =
      "export data OPTIONS( "
          + "uri='<exportPath>/<table>-*.parquet', "
          + "format='PARQUET') "
          + "AS (<exportToParquetQuery>)";

  private static final String exportPathTemplate = "gs://<bucket>/<flightId>/<table>";
  private static final String exportToParquetQueryTemplate =
      "select * from `<project>.<snapshot>.<table>`";

  public List<String> exportTableToParquet(
      Snapshot snapshot,
      GoogleBucketResource bucketResource,
      String flightId,
      boolean exportGsPaths)
      throws InterruptedException {
    List<String> paths = new ArrayList<>();
    BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);

    for (var table : snapshot.getTables()) {
      String tableName = table.getName();
      String exportPath =
          new ST(exportPathTemplate)
              .add("bucket", bucketResource.getName())
              .add("flightId", flightId)
              .add("table", tableName)
              .render();

      final String exportToParquetQuery;
      if (exportGsPaths) {
        exportToParquetQuery = createExportToParquetWithGsPathQuery(snapshot, table, flightId);
      } else {
        exportToParquetQuery = creteExportToParquetQuery(snapshot, table, flightId);
      }

      String exportStatement =
          new ST(exportToParquetTemplate)
              .add("exportPath", exportPath)
              .add("exportToParquetQuery", exportToParquetQuery)
              .add("table", tableName)
              .render();

      bigQueryProject.query(exportStatement);
      paths.add(exportPath);
    }

    return paths;
  }

  private String creteExportToParquetQuery(
      Snapshot snapshot, SnapshotTable table, String flightId) {
    String snapshotProject = snapshot.getProjectResource().getGoogleProjectId();
    String snapshotName = snapshot.getName();
    return new ST(exportToParquetQueryTemplate)
        .add("table", table.getName())
        .add("project", snapshotProject)
        .add("snapshot", snapshotName)
        .render();
  }

  private static final String exportToMappingTableTemplate =
      "WITH datarepo_gs_path_mapping AS "
          + "(SELECT <fileIdKey>, <gsPathKey> "
          + "  FROM `<project>.<snapshotDatasetName>.<gsPathMappingTable>`) "
          + "SELECT "
          + "  S.<pdaoRowIdColumn>,"
          + "  <mappedColumns; separator=\",\"> "
          + "FROM "
          + "`<project>.<snapshotDatasetName>.<table>` S";

  private String createExportToParquetWithGsPathQuery(
      Snapshot snapshot, SnapshotTable table, String flightId) {
    String gsPathMappingTableName = BigQueryUtils.gsPathMappingTableName(snapshot);
    String extTableName = externalTableName(gsPathMappingTableName, flightId);
    List<String> mappedColumns =
        table.getColumns().stream().map(this::gsPathMappingSelectSql).collect(Collectors.toList());

    return new ST(exportToMappingTableTemplate)
        .add("project", snapshot.getProjectResource().getGoogleProjectId())
        .add("snapshotDatasetName", snapshot.getName())
        .add("gsPathMappingTable", extTableName)
        .add("pdaoRowIdColumn", PDAO_ROW_ID_COLUMN)
        .add("mappedColumns", mappedColumns)
        .add("table", table.getName())
        .add("gsPathKey", PDAO_FIRESTORE_DUMP_GSPATH_KEY)
        .add("fileIdKey", PDAO_FIRESTORE_DUMP_FILE_ID_KEY)
        .render();
  }

  private static final String fileRefMappingColumnTemplate =
      "(SELECT <gsPathKey> "
          + "FROM datarepo_gs_path_mapping "
          + "WHERE RIGHT(S.<columnName>, 36) = <fileIdKey>) AS <columnName>";

  private static final String fileRefArrayOfMappingColumnTemplate =
      "  ARRAY(SELECT <gsPathKey> "
          + "    FROM UNNEST(<columnName>) AS unnested_drs_path, "
          + "    datarepo_gs_path_mapping "
          + "    WHERE RIGHT(unnested_drs_path, 36) = <fileIdKey>) AS <columnName>";

  private String gsPathMappingSelectSql(Column snapshotColumn) {
    String columnName = snapshotColumn.getName();
    if (snapshotColumn.isFileOrDirRef()) {
      final String columnTemplate;
      if (snapshotColumn.isArrayOf()) {
        columnTemplate = fileRefArrayOfMappingColumnTemplate;
      } else {
        columnTemplate = fileRefMappingColumnTemplate;
      }
      return new ST(columnTemplate)
          .add("columnName", columnName)
          .add("gsPathKey", PDAO_FIRESTORE_DUMP_GSPATH_KEY)
          .add("fileIdKey", PDAO_FIRESTORE_DUMP_FILE_ID_KEY)
          .render();
    } else {
      return "S." + snapshotColumn.getName();
    }
  }

  static long getSingleLongValue(TableResult result) {
    FieldValueList fieldValues = result.getValues().iterator().next();
    return fieldValues.get(0).getLongValue();
  }
}
