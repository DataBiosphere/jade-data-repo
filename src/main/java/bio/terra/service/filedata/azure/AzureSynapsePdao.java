package bio.terra.service.filedata.azure;

import static bio.terra.service.filedata.azure.util.BlobContainerClientFactory.SASPermission;

import bio.terra.common.Column;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobUrlParts;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
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

  private final AzureResourceConfiguration azureResourceConfiguration;
  private final AzureBlobStorePdao azureBlobStorePdao;

  @Autowired
  public AzureSynapsePdao(
      AzureResourceConfiguration azureResourceConfiguration,
      AzureBlobStorePdao azureBlobStorePdao) {
    this.azureResourceConfiguration = azureResourceConfiguration;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  public void createExternalDataSource(
      String ingestControlFilePath,
      UUID tenantId,
      String scopedCredentialName,
      String dataSourceName,
      SASPermission permissionType)
      throws NotImplementedException, SQLException {

    BlobUrlParts original_blobUrlParts = BlobUrlParts.parse(ingestControlFilePath);
    String blobName = original_blobUrlParts.getBlobName();
    BlobContainerClientFactory sourceClientFactory =
        azureBlobStorePdao.getOrBuildClientFactory(tenantId, original_blobUrlParts);

    String signedURL = sourceClientFactory.createSasUrlForBlob(blobName, permissionType);

    BlobUrlParts blobUrl = BlobUrlParts.parse(signedURL);
    AzureSasCredential blobContainerSasTokenCreds =
        new AzureSasCredential(blobUrl.getCommonSasQueryParameters().encode());

    runAQuery(
        "CREATE DATABASE SCOPED CREDENTIAL ["
            + scopedCredentialName
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
            + scopedCredentialName
            + "]);");
  }

  public void createParquetFiles(
      DatasetTable datasetTable,
      String ingestFileName,
      String destinationParquetFile,
      String destinationDataSourceName,
      String controlFileDataSourceName,
      String ingestTableName)
      throws SQLException {
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
            + destinationDataSourceName
            + "],\n    "
            + "FILE_FORMAT = ["
            + PARQUET_FILE_FORMAT_NAME
            + "]\n"
            + ") AS SELECT * FROM OPENROWSET(BULK '"
            + ingestFileName
            + "',\n                                "
            + "DATA_SOURCE = '"
            + controlFileDataSourceName
            + "',\n                                "
            + "FORMAT='CSV'" // TODO - switch on control file
            + ",\n                                "
            + "PARSER_VERSION = '2.0'"
            + ",\n                                "
            + "FIRSTROW = 2)\n" // TODO - allow this as input
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

  public void cleanSynapseEntries(
      List<String> tableNames, List<String> dataSourceNames, List<String> credentialNames) {
    tableNames.stream()
        .forEach(
            tableName -> {
              try {
                runAQuery("DROP EXTERNAL TABLE [" + tableName + "];");
              } catch (Exception ex) {
                logger.warn("Unable to clean up table {}, ex: {}", tableName, ex.getMessage());
              }
            });
    dataSourceNames.stream()
        .forEach(
            dataSource -> {
              try {
                runAQuery("DROP EXTERNAL DATA SOURCE [" + dataSource + "];");
              } catch (Exception ex) {
                logger.warn(
                    "Unable to clean up the external data source {}, ex: {}",
                    dataSource,
                    ex.getMessage());
              }
            });
    credentialNames.stream()
        .forEach(
            credential -> {
              try {
                runAQuery("DROP DATABASE SCOPED CREDENTIAL [" + credential + "];");
              } catch (Exception ex) {
                logger.warn(
                    "Unable to clean up scoped credential {}, ex: {}", credential, ex.getMessage());
              }
            });
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
}
