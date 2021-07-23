package bio.terra.service.filedata.azure;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class AzureSynapsePdao {
  private static final Logger logger = LoggerFactory.getLogger(AzureSynapsePdao.class);

  private static final String DB_NAME = "datarepo";
  private static final String GS_PROTOCOL = "abfs://";
  private static final String GS_BUCKET_PATTERN = "[a-z0-9_.\\-]{3,222}";

  private final GcsProjectFactory gcsProjectFactory;
  private final ResourceService resourceService;
  private final FireStoreDao fileDao;
  private final ConfigurationService configurationService;
  private final ExecutorService executor;
  private final PerformanceLogger performanceLogger;
  private final AzureResourceConfiguration azureResourceConfiguration;
  private final ObjectMapper objectMapper;
  private final ApplicationConfiguration applicationConfiguration;
  private final DrsService drsService;

  @Autowired
  public AzureSynapsePdao(
      GcsProjectFactory gcsProjectFactory,
      ResourceService resourceService,
      FireStoreDao fileDao,
      ConfigurationService configurationService,
      AzureResourceConfiguration azureResourceConfiguration,
      @Qualifier("performanceThreadpool") ExecutorService executor,
      PerformanceLogger performanceLogger,
      ObjectMapper objectMapper,
      ApplicationConfiguration applicationConfiguration,
      DrsService drsService) {
    this.gcsProjectFactory = gcsProjectFactory;
    this.resourceService = resourceService;
    this.fileDao = fileDao;
    this.configurationService = configurationService;
    this.executor = executor;
    this.performanceLogger = performanceLogger;
    this.azureResourceConfiguration = azureResourceConfiguration;
    this.objectMapper = objectMapper;
    this.applicationConfiguration = applicationConfiguration;
    this.drsService = drsService;
  }

  public boolean runAQuery(String query) throws SQLException {
    SQLServerDataSource ds = getDatasource();
    try (Connection connection = ds.getConnection()) {
      // Update or create the credential
      try (Statement statement = connection.createStatement()) {
        return statement.execute(query);
      }
    }
  }

  private SQLServerDataSource getDatasource() {
    SQLServerDataSource ds = new SQLServerDataSource();
    ds.setServerName(azureResourceConfiguration.getSynapse().getWorkspaceName());
    ds.setUser(azureResourceConfiguration.getSynapse().getSqlAdminUser());
    ds.setPassword(azureResourceConfiguration.getSynapse().getSqlAdminPassword());
    ds.setDatabaseName(DB_NAME);
    return ds;
  }
  //    public void createDatasetExternalTables(
  //        bio.terra.model.BillingProfileModel billingProfileModel,
  //        GoogleBucketResource bucketResource,
  //        Dataset dataset) {
  //
  //      SQLServerDataSource ds = getDatasource();
  //
  //      // TODO move the following to the drs service? we only have the google option now
  //      String path =
  //          AzureStoragePdao.generateAdfsBasePath(bucketResource)
  //              + AzureStoragePdao.generateAdfsDsPath(dataset);
  //      String sasToken =
  //          storagePdao.signUrl(
  //              path,
  //              billingProfileModel.getAzureSubscription(),
  //              dataset.getProjectResource().getAzureResourceGroupName(),
  //              true);
  //      // drsService.getAccessUrlForObjectId();
  //      String id =
  //          dataset
  //              .getId()
  //              .toString()
  //              .replaceAll("-", ""); // Long.toString(Instant.now().toEpochMilli());
  //      String credentialName = "cred" + id;
  //      String datasourceName = "ds" + id;
  //      String formatName = "fmt" + id;
  //      String tableName = "tab" + id;
  //      String datasourceLocation =
  //          "https://"
  //              + bucketResource.getAccountName()
  //              + ".blob.core.windows.net/"
  //              + dataset.getProjectResource().getAzureStorageAccountName()
  //              + "/";
  //
  //      logger.info("Cred: {}\nDatasource: {}\nFormat: {}", credentialName, datasourceName,
  //   formatName);
  //      // Create the DB if it doesn't exist
  //      createDatabase(dataset.getProjectResource());
  //
  //      try (Connection connection = ds.getConnection()) {
  //        // Update or create the credential
  //        try (Statement statement = connection.createStatement()) {
  //          statement.execute(
  //              ""
  //                  + "CREATE DATABASE SCOPED CREDENTIAL "
  //                  + credentialName
  //                  + " \n"
  //                  + "WITH IDENTITY = 'SHARED ACCESS SIGNATURE', \n"
  //                  + "SECRET = '"
  //                  + sasToken
  //                  + "'");
  //        }
  //
  //        try (Statement statement = connection.createStatement()) {
  //          statement.execute(
  //              ""
  //                  + "CREATE EXTERNAL DATA SOURCE "
  //                  + datasourceName
  //                  + "\n"
  //                  + "WITH\n"
  //                  + "    (\n"
  //                  + "        LOCATION = '"
  //                  + datasourceLocation
  //                  + "' ,\n"
  //                  + "        CREDENTIAL = "
  //                  + credentialName
  //                  + "\n"
  //                  + "    )");
  //        }
  //
  //        try (Statement statement = connection.createStatement()) {
  //          statement.execute(
  //              ""
  //                  + "CREATE EXTERNAL FILE FORMAT "
  //                  + formatName
  //                  + "\n"
  //                  + "WITH (  \n"
  //                  + "    FORMAT_TYPE = PARQUET,\n"
  //                  + "    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'\n"
  //                  + ")");
  //        }
  //
  //        for (DatasetTable datasetTable : dataset.getTables()) {
  //          String tableLocation =
  //              AzureStoragePdao.generateAdfsDsTableFlightPath(dataset, datasetTable, "*") +
  //   "/PART-*";
  //          try (Statement statement = connection.createStatement()) {
  //            statement.execute(
  //                ""
  //                    + "CREATE EXTERNAL TABLE "
  //                    + tableName
  //                    + " (\n"
  //                    + "    datarepo_row_id VARCHAR(250),\n"
  //                    + "    name VARCHAR(250),\n"
  //                    + "    file_ref VARCHAR(250))\n"
  //                    + "WITH\n"
  //                    + "(\n"
  //                    + "    LOCATION = '"
  //                    + tableLocation
  //                    + "',\n"
  //                    + "    DATA_SOURCE = "
  //                    + datasourceName
  //                    + ",\n"
  //                    + "    FILE_FORMAT = "
  //                    + formatName
  //                    + "\n"
  //                    + ")");
  //          }
  //        }
  //
  //      } catch (Exception exception) {
  //        throw new RuntimeException("Error running SQL", exception);
  //      }
  //      //        // Connect to Synapse
  //      //        new ArtifactsClientBuilder()
  //      //            .endpoint("https://" + bucketResource.getAccountName() +
  //   ".dev.azuresynapse.net")
  //      //            .credential(azureResourceConfiguration.getAppToken(tenantId))
  //
  //      //        sqlScriptClient.getSqlScript("get")
  //
  //      //        sqlPoolsClient.get(bucketResource.getAccountName());
  //
  //    }

  //  public void createSnapshotFiles(
  //      BillingProfileModel billingProfileModel,
  //      GoogleBucketResource bucketResource,
  //      Snapshot snapshot,
  //      SnapshotRequestRowIdModel rowIdModel) {
  //
  //    SQLServerDataSource ds = getDatasource(snapshot.getProjectResource(), DB_NAME);
  //
  //    // TODO: assuming a single bucket resource for now.
  //    // TODO: assuming a single dataset.  Convert to a loop when we support multiple datasets
  //    Dataset dataset = snapshot.getSnapshotSources().get(0).getDataset();
  //    String sourcePath =
  //        AzureStoragePdao.generateAdfsBasePath(bucketResource)
  //            + AzureStoragePdao.generateAdfsDsPath(dataset);
  //    // TODO: Should we create a Sas key for each table?  Maybe but would require different
  // directory
  //    // layout
  //    String targetPath =
  //        AzureStoragePdao.generateAdfsBasePath(bucketResource)
  //            + AzureStoragePdao.generateAdfsSnapshotPath(snapshot);
  //
  //    String sourceSasToken =
  //        storagePdao.signUrl(
  //            sourcePath,
  //            billingProfileModel.getAzureSubscription(),
  //            dataset.getProjectResource().getAzureResourceGroupName(),
  //            true);
  //    String targetSasToken =
  //        storagePdao.signUrl(
  //            targetPath,
  //            billingProfileModel.getAzureSubscription(),
  //            dataset.getProjectResource().getAzureResourceGroupName(),
  //            true,
  //            true);
  //    String targetId = snapshot.getId().toString().replaceAll("-", "");
  //    String sourceId = targetId + "_" + dataset.getId().toString().replaceAll("-", "");
  //    // Max 116 chars
  //    String sourceCredentialName = "cred" + sourceId;
  //    String sourceDatasourceName = "ds" + sourceId;
  //    String sourceFormatName = "fmt" + sourceId;
  //    String sourceDatasourceLocation =
  //        "https://"
  //            + bucketResource.getAccountName()
  //            + ".blob.core.windows.net/"
  //            + bucketResource.getName()
  //            + "/";
  //
  //    String targetCredentialName = "cred" + targetId;
  //    String targetDatasourceName = "ds" + targetId;
  //    String targetFormatName = "fmt" + targetId;
  //    String targetDatasourceLocation =
  //        "https://"
  //            + bucketResource.getAccountName()
  //            + ".blob.core.windows.net/"
  //            + bucketResource.getName()
  //            + "/";
  //
  //    List<String> drops = new ArrayList<>();
  //    //        logger.info("Cred: {}\nDatasource: {}\nFormat: {}", credentialName,
  // datasourceName,
  //    // formatName);
  //
  //    // Create the DB if it doesn't exist
  //    createDatabase(snapshot.getProjectResource());
  //
  //    try (Connection connection = ds.getConnection()) {
  //      // Update or create the credential
  //      executeSql(
  //          connection,
  //          ""
  //              + "CREATE DATABASE SCOPED CREDENTIAL "
  //              + sourceCredentialName
  //              + " \n"
  //              + "WITH IDENTITY = 'SHARED ACCESS SIGNATURE', \n"
  //              + "SECRET = '"
  //              + sourceSasToken
  //              + "'");
  //      drops.add("drop database scoped credential " + sourceCredentialName + ";");
  //
  //      executeSql(
  //          connection,
  //          ""
  //              + "CREATE EXTERNAL DATA SOURCE "
  //              + sourceDatasourceName
  //              + "\n"
  //              + "WITH\n"
  //              + "    (\n"
  //              + "        LOCATION = '"
  //              + sourceDatasourceLocation
  //              + "' ,\n"
  //              + "        CREDENTIAL = "
  //              + sourceCredentialName
  //              + "\n"
  //              + "    )");
  //      drops.add("drop external data source " + sourceDatasourceName + ";");
  //
  //      executeSql(
  //          connection,
  //          ""
  //              + "CREATE EXTERNAL FILE FORMAT "
  //              + sourceFormatName
  //              + "\n"
  //              + "WITH (  \n"
  //              + "    FORMAT_TYPE = PARQUET,\n"
  //              + "    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'\n"
  //              + ")");
  //      drops.add("drop external file format " + sourceFormatName + ";");
  //
  //      // Create source tables
  //      for (DatasetTable datasetTable : dataset.getTables()) {
  //        String tableLocation =
  //            AzureStoragePdao.generateAdfsDsTableFlightPath(dataset, datasetTable, "*") +
  // "/PART-*";
  //        String sourceTableName = makeSourceTable(snapshot, datasetTable.getName());
  //        List<String> columnsToInclude = getColumnsToInclude(datasetTable.getName(), rowIdModel);
  //        executeSql(
  //            connection,
  //            ""
  //                + "CREATE EXTERNAL TABLE "
  //                + sourceTableName
  //                + " (\n"
  //                + "    "
  //                + getTableSignature(datasetTable.getColumns(), columnsToInclude)
  //                + "\n"
  //                + ")\n"
  //                + "WITH\n"
  //                + "(\n"
  //                + "    LOCATION = '"
  //                + tableLocation
  //                + "',\n"
  //                + "    DATA_SOURCE = "
  //                + sourceDatasourceName
  //                + ",\n"
  //                + "    FILE_FORMAT = "
  //                + sourceFormatName
  //                + "\n"
  //                + ")");
  //        drops.add("drop external table " + sourceTableName + ";");
  //      }
  //
  //      // Create snapshot artifacts
  //      executeSql(
  //          connection,
  //          ""
  //              + "CREATE DATABASE SCOPED CREDENTIAL "
  //              + targetCredentialName
  //              + " \n"
  //              + "WITH IDENTITY = 'SHARED ACCESS SIGNATURE', \n"
  //              + "SECRET = '"
  //              + targetSasToken
  //              + "'");
  //      drops.add("drop database scoped credential " + targetCredentialName + ";");
  //
  //      executeSql(
  //          connection,
  //          ""
  //              + "CREATE EXTERNAL DATA SOURCE "
  //              + targetDatasourceName
  //              + "\n"
  //              + "WITH\n"
  //              + "    (\n"
  //              + "        LOCATION = '"
  //              + targetDatasourceLocation
  //              + "' ,\n"
  //              + "        CREDENTIAL = "
  //              + targetCredentialName
  //              + "\n"
  //              + "    )");
  //      drops.add("drop external data source " + targetDatasourceName + ";");
  //
  //      executeSql(
  //          connection,
  //          ""
  //              + "CREATE EXTERNAL FILE FORMAT "
  //              + targetFormatName
  //              + "\n"
  //              + "WITH (  \n"
  //              + "    FORMAT_TYPE = PARQUET,\n"
  //              + "    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'\n"
  //              + ")");
  //      drops.add("drop external file format " + targetFormatName + ";");
  //
  //      // Create source tables
  //      for (SnapshotTable snapshotTable : snapshot.getTables()) {
  //        String tableLocation =
  //            AzureStoragePdao.generateAdfsSnapshotTableFlightPath(snapshot, snapshotTable,
  // "all");
  //        String targetTableName = makeSnapshotTable(snapshot, snapshotTable.getName());
  //        String sourceTableName = makeSourceTable(snapshot, snapshotTable.getName());
  //        List<String> columnsToInclude = getColumnsToInclude(snapshotTable.getName(),
  // rowIdModel);
  //        executeSql(
  //            connection,
  //            ""
  //                + "CREATE EXTERNAL TABLE "
  //                + targetTableName
  //                + "\n"
  //                + "WITH\n"
  //                + "(\n"
  //                + "    LOCATION = '"
  //                + tableLocation
  //                + "',\n"
  //                + "    DATA_SOURCE = "
  //                + targetDatasourceName
  //                + ",\n"
  //                + "    FILE_FORMAT = "
  //                + targetFormatName
  //                + "\n"
  //                + ") AS SELECT "
  //                + getSelectClause(snapshot, snapshotTable, columnsToInclude)
  //                + "\n"
  //                + "     FROM "
  //                + sourceTableName
  //                + " AS "
  //                + snapshotTable.getName()
  //                + "\n"
  //                + "    WHERE "
  //                + getWhereClause(snapshotTable, rowIdModel));
  //        drops.add("drop external table " + targetTableName + ";");
  //      }
  //
  //    } catch (Exception exception) {
  //      Collections.reverse(drops);
  //      logger.info(
  //          "************************************\n" + "RUN THE FOLLOWING CLEANUP:\n{}",
  //          String.join("\n", drops));
  //      throw new RuntimeException("Error running SQL", exception);
  //    }
  //
  //    Collections.reverse(drops);
  //    logger.info("RUN THE FOLLOWING CLEANUP:\n{}", String.join("\n", drops));
  //
  //    //        // Connect to Synapse
  //    //        new ArtifactsClientBuilder()
  //    //            .endpoint("https://" + bucketResource.getAccountName() +
  // ".dev.azuresynapse.net")
  //    //            .credential(azureResourceConfiguration.getAppToken(tena   ntId))
  //
  //    //        sqlScriptClient.getSqlScript("get")
  //
  //    //        sqlPoolsClient.get(bucketResource.getAccountName());
  //
  //  }
  //
  //  private String makeSourceTable(Snapshot snapshot, String datasetTableName) {
  //    return "src"
  //        + snapshot.getId().toString().replaceAll("-", "")
  //        + "_"
  //        + datasetTableName.toLowerCase();
  //  }
  //
  //  private String makeSnapshotTable(Snapshot snapshot, String snapshotTableName) {
  //    return "snp"
  //        + snapshot.getId().toString().replaceAll("-", "")
  //        + "_"
  //        + snapshotTableName.toLowerCase();
  //  }
  //
  //  private void executeSql(Connection connection, String sql) {
  //    try (Statement statement = connection.createStatement()) {
  //      logger.info("\n\nRunning:\n{}\n\n", sql);
  //      statement.execute(sql);
  //    } catch (SQLException e) {
  //      throw new RuntimeException("Error running SQL", e);
  //    }
  //  }
  //
  //  private List<String> getColumnsToInclude(String table, SnapshotRequestRowIdModel rowIdModel) {
  //    if (rowIdModel != null) {
  //      return rowIdModel.getTables().stream()
  //          .filter(t -> t.getTableName().equalsIgnoreCase(table))
  //          .findFirst()
  //          .map(t ->
  // t.getColumns().stream().map(String::toLowerCase).collect(Collectors.toList()))
  //          .orElse(null);
  //    } else {
  //      return null;
  //    }
  //  }
  //
  //  private String getTableSignature(List<Column> columnList, List<String> columnsToInclude) {
  //    return PDAO_ROW_ID_COLUMN
  //        + " VARCHAR(250),\n"
  //        + columnList.stream()
  //            .filter(
  //                c ->
  //                    columnsToInclude == null
  //                        || columnsToInclude.contains(c.getName().toLowerCase()))
  //            .map(c -> "    " + c.getName() + " " + translateTypeToDdl(c.getType(),
  // c.isArrayOf()))
  //            .collect(Collectors.joining(",\n"));
  //  }
  //
  //  private String getSelectClause(
  //      Snapshot snapshot, SnapshotTable snapshotTable, List<String> columnsToInclude) {
  //    return PDAO_ROW_ID_COLUMN
  //        + ", "
  //        + snapshotTable.getColumns().stream()
  //            .filter(
  //                c ->
  //                    columnsToInclude == null
  //                        || columnsToInclude.contains(c.getName().toLowerCase()))
  //            .map(
  //                c -> {
  //                  if (Arrays.asList("DIRREF", "FILEREF").contains(c.getType().toUpperCase())) {
  //                    return "'drs://"
  //                        + applicationConfiguration.getDnsName()
  //                        + "/v1_"
  //                        + snapshot.getId().toString()
  //                        + "_' + "
  //                        + c.getName()
  //                        + " AS "
  //                        + c.getName();
  //                  }
  //                  return c.getName();
  //                })
  //            .collect(Collectors.joining(", "));
  //  }
  //
  //  private String getWhereClause(SnapshotTable snapshotTable, SnapshotRequestRowIdModel
  // rowIdModel)
  // {
  //    if (rowIdModel != null) {
  //      List<String> rowIds =
  //          rowIdModel.getTables().stream()
  //              .filter(t -> t.getTableName().equalsIgnoreCase(snapshotTable.getName()))
  //              .findFirst()
  //              .map(SnapshotRequestRowIdTableModel::getRowIds)
  //              .orElseGet(Collections::emptyList);
  //      if (rowIds.isEmpty()) {
  //        return "1 = 1";
  //      } else {
  //        // Detect if any of these are not valid UUIDs to avoid SQL injection. Could also
  // validate
  //        // ahead of time
  //        try {
  //          rowIds.forEach(UUID::fromString);
  //        } catch (IllegalArgumentException e) {
  //          throw new RuntimeException("Nice try!  Bad row Id(s)", e);
  //        }
  //        return snapshotTable.getName()
  //            + ".datarepo_row_id IN ("
  //            + rowIds.stream().map(i -> "'" + i + "'").collect(Collectors.joining(","))
  //            + ")";
  //      }
  //    } else {
  //      return "1 = 1";
  //    }
  //  }
  //

  //
  //  public Map<String, Long> getSnapshotTableRowCounts(Snapshot snapshot) {
  //    SQLServerDataSource ds = getDatasource(snapshot.getProjectResource(), DB_NAME);
  //    Map<String, Long> counts = new HashMap<>();
  //    try (Connection connection = ds.getConnection();
  //        Statement statement = connection.createStatement()) {
  //      // TODO Assuming a single dataset
  //      for (SnapshotTable snapshotTable : snapshot.getTables()) {
  //        String tableName =
  //            "snp"
  //                + snapshot.getId().toString().replaceAll("-", "")
  //                + "_"
  //                + snapshotTable.getName().toLowerCase();
  //        try (ResultSet resultSet =
  //            statement.executeQuery("SELECT COUNT(*) cnt FROM " + tableName)) {
  //          if (resultSet.next()) {
  //            counts.put(snapshotTable.getName(), resultSet.getLong("cnt"));
  //          }
  //        }
  //      }
  //
  //    } catch (Exception exception) {
  //      throw new RuntimeException("Error running SQL", exception);
  //    }
  //
  //    return counts;
  //  }
  //
  //  private static final String getSnapshotRefIdsTemplate =
  //      "SELECT S.<refCol> refId FROM <datasetTable> S, "
  //          + "<snapshotTable> R "
  //          +
  //          //            "<if(array)>CROSS JOIN UNNEST(S.<refCol>) AS <refCol> <endif>" +
  //          "WHERE S."
  //          + PDAO_ROW_ID_COLUMN
  //          + " = R."
  //          + PDAO_ROW_ID_COLUMN;
  //  //        "SELECT <refCol> FROM `<project>.<dataset>.<table>` S, " +
  //  //            "`<project>.<snapshot>." + PDAO_ROW_ID_TABLE + "` R " +
  //  //            "<if(array)>CROSS JOIN UNNEST(S.<refCol>) AS <refCol> <endif>" +
  //  //            "WHERE S." + PDAO_ROW_ID_COLUMN + " = R." + PDAO_ROW_ID_COLUMN + " AND " +
  //  //            "R." + PDAO_TABLE_ID_COLUMN + " = '<tableId>'";
  //
  //  public List<String> getSnapshotRefIds(Snapshot snapshot, String tableName, Column refColumn) {
  //    SQLServerDataSource ds = getDatasource(snapshot.getProjectResource(), DB_NAME);
  //
  //    ST sqlTemplate = new ST(getSnapshotRefIdsTemplate);
  //    sqlTemplate.add("datasetTable", makeSourceTable(snapshot, tableName));
  //    sqlTemplate.add("snapshotTable", makeSnapshotTable(snapshot, tableName));
  //    sqlTemplate.add("refCol", refColumn.getName());
  //    List<String> refIdArray = new ArrayList<>();
  //
  //    try (Connection connection = ds.getConnection();
  //        Statement statement = connection.createStatement()) {
  //      try (ResultSet resultSet = statement.executeQuery(sqlTemplate.render())) {
  //        while (resultSet.next()) {
  //          String refId = resultSet.getString("refId");
  //          if (refId != null) {
  //            refIdArray.add(refId);
  //          }
  //        }
  //      }
  //    } catch (Exception exception) {
  //      throw new RuntimeException("Error running SQL", exception);
  //    }
  //
  //    return refIdArray;
  //  }
  //
  //  // TODO: Make an enum for the datatypes in swagger
  //  private String translateTypeToDdl(String datatype, boolean isArrayOf) {
  //    // TODO: test this
  //    if (isArrayOf) {
  //      return "varchar(8000)";
  //    }
  //    String uptype = StringUtils.upperCase(datatype);
  //    switch (uptype) {
  //      case "BOOLEAN":
  //        return "bit";
  //      case "BYTES":
  //        return "varbinary";
  //      case "DATE":
  //        return "date";
  //      case "DATETIME":
  //        return "datetime2";
  //      case "DIRREF":
  //        return "varchar(250)";
  //      case "FILEREF":
  //        return "varchar(250)";
  //      case "FLOAT":
  //        return "real";
  //      case "FLOAT64":
  //        return "real";
  //      case "INTEGER":
  //        return "int";
  //      case "INT64":
  //        return "bigint";
  //      case "NUMERIC":
  //        return "decimal";
  //        // case "RECORD":    return LegacySQLTypeName.RECORD;
  //      case "STRING":
  //        return "varchar(8000)";
  //      case "TEXT":
  //        return "varchar(8000)"; // match the Postgres type
  //      case "TIME":
  //        return "time";
  //      case "TIMESTAMP":
  //        return "datetime2";
  //      default:
  //        throw new IllegalArgumentException("Unknown datatype '" + datatype + "'");
  //    }
  //  }
}
