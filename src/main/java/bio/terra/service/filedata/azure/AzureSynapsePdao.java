package bio.terra.service.filedata.azure;

import bio.terra.common.Column;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobUrlParts;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureSynapsePdao {
  private static final Logger logger = LoggerFactory.getLogger(AzureSynapsePdao.class);

  // TODO - Move db_name & parquet format name into app properties
  private static final String DB_NAME = "datarepo";
  private static final String PARQUET_FILE_FORMAT_NAME = "ParquetFileFormat";
  // Static prefixes for temp variables in synapse
  private static final String SAS_TOKEN_PREFIX = "sas_";
  private static final String DATA_SOURCE_PREFIX = "ds_";
  private static final String TABLE_NAME_PREFIX = "ingest_";

  private final AzureResourceConfiguration azureResourceConfiguration;

  @Autowired
  public AzureSynapsePdao(AzureResourceConfiguration azureResourceConfiguration) {
    this.azureResourceConfiguration = azureResourceConfiguration;
  }

  // -----TODO - Remove. Copied over from Muscle's PR----
  private static final Set<String> VALID_TLDS =
      Set.of("blob.core.windows.net", "dfs.core.windows.net");

  static boolean isSignedUrl(String url) {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(url);

    if (VALID_TLDS.stream().noneMatch(h -> blobUrlParts.getHost().toLowerCase().endsWith(h))) {
      return false;
    }
    return !StringUtils.isEmpty(blobUrlParts.getCommonSasQueryParameters().getSignature());
  }
  // --- end of copied code ---

  public void createExternalDataSource(String ingestControlFilePath, String flightId)
      throws NotImplementedException, SQLException {
    BlobUrlParts blobUrl = BlobUrlParts.parse(ingestControlFilePath);
    AzureSasCredential blobContainerSasTokenCreds;
    if (isSignedUrl(ingestControlFilePath)) {
      blobContainerSasTokenCreds =
          new AzureSasCredential(blobUrl.getCommonSasQueryParameters().encode());
    } else {
      // TODO - sign urls if not already provided
      throw new NotImplementedException("Add implementation to handle urls without signature");
    }

    String sasTokenName = getSasTokenName(flightId);
    String dataSourceName = getDataSourceName(flightId);

    runAQuery(
        "CREATE DATABASE SCOPED CREDENTIAL ["
            + sasTokenName
            + "]\n"
            + "WITH IDENTITY = 'SHARED ACCESS SIGNATURE',\n"
            + "SECRET = '"
            + blobContainerSasTokenCreds.getSignature()
            + "';");

    runAQuery(
        "CREATE EXTERNAL DATA SOURCE "
            + "["
            + dataSourceName
            + "]\n"
            + "WITH (\n"
            + "LOCATION = '"
            + blobUrl.getScheme()
            + "://"
            + blobUrl.getHost()
            + "/"
            + blobUrl.getBlobContainerName()
            + "',\n"
            + "CREDENTIAL = ["
            + sasTokenName
            + "]);");
  }

  public void createParquetFiles(
      DatasetTable datasetTable,
      String ingestFileName,
      String destinationParquetFile,
      String flightId)
      throws SQLException {
    String dataSourceName = getDataSourceName(flightId);
    String ingestTableName = getTableName(flightId);

    // build the ingest request
    runAQuery(
        "CREATE EXTERNAL TABLE ["
            + ingestTableName
            + "]\n"
            + "WITH (\n    "
            + "LOCATION = '"
            + destinationParquetFile
            + "',\n    "
            + "DATA_SOURCE = ["
            + dataSourceName
            + "],\n    "
            + "FILE_FORMAT = ["
            + PARQUET_FILE_FORMAT_NAME
            + "]\n"
            + ") AS SELECT * FROM OPENROWSET(BULK '"
            + ingestFileName
            + "',\n                                "
            + "DATA_SOURCE = '"
            + dataSourceName
            + "',\n                                "
            + "FORMAT='CSV'" // TODO - switch on control file
            +",\n                                "
            + "PARSER_VERSION = '2.0'"
            + ",\n                                "
            + "FIRSTROW = 2)\n" //TODO - allow this as input
            + "WITH (\n      "
            + buildTableSchema(datasetTable)
            + ") AS rows;");
  }

  public String buildTableSchema(DatasetTable datasetTable) {
    List<Column> columns = datasetTable.getColumns();
    return String.join(
        ",\n      ",
        columns.stream()
            .map(c -> c.getName() + " " + translateTypeToDdl(c.getType(), c.isArrayOf()))
            .collect(Collectors.toList()));
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

  public void cleanSynapseEntries(String flightId) {
    String sasTokenName = getSasTokenName(flightId);
    String dataSourceName = getDataSourceName(flightId);
    String ingestTableName = getTableName(flightId);
    try {
      runAQuery("DROP EXTERNAL TABLE [" + ingestTableName + "];");
    } catch (Exception ex) {
      logger.warn("Unable to clean up table for flight {}, ex: {}", flightId, ex.getMessage());
    }
    try {
      runAQuery("DROP EXTERNAL DATA SOURCE [" + dataSourceName + "];");
    } catch (Exception ex) {
      logger.warn(
          "Unable to clean up external data source for flight {}, ex: {}",
          flightId,
          ex.getMessage());
    }
    try {
      runAQuery("DROP DATABASE SCOPED CREDENTIAL [" + sasTokenName + "];");
    } catch (Exception ex) {
      logger.warn(
          "Unable to clean up scoped credential for flight {}, ex: {}", flightId, ex.getMessage());
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

  // TODO - test all data types
  private String translateTypeToDdl(TableDataType datatype, boolean isArrayOf) {
    if (isArrayOf) {
      return "varchar(8000) COLLATE Latin1_General_100_CI_AI_SC_UTF8";
    }
    switch (datatype) {
      case BOOLEAN:
        return "bit";
      case BYTES:
        return "varbinary";
      case DATE:
        return "date";
      case DATETIME:
      case TIMESTAMP:
        return "datetime2";
      case DIRREF:
      case FILEREF:
        return "varchar(250) COLLATE Latin1_General_100_CI_AI_SC_UTF8";
      case FLOAT:
      case FLOAT64:
        return "real";
      case INTEGER:
        return "int";
      case INT64:
        return "bigint";
      case NUMERIC:
        return "decimal";
        // case "RECORD":    return LegacySQLTypeName.RECORD;
      case TEXT:
      case STRING:
        return "varchar(8000) COLLATE Latin1_General_100_CI_AI_SC_UTF8";
      case TIME:
        return "time";
      default:
        throw new IllegalArgumentException("Unknown datatype '" + datatype + "'");
    }
  }

  private String getSasTokenName(String flightId) {
    return SAS_TOKEN_PREFIX + flightId;
  }

  private String getDataSourceName(String flightId) {
    return DATA_SOURCE_PREFIX + flightId;
  }

  private String getTableName(String flightId) {
    return TABLE_NAME_PREFIX + flightId;
  }
}
