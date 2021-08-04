package bio.terra.service.filedata.azure;

import static bio.terra.service.filedata.azure.util.BlobContainerClientFactory.SasPermission;

import bio.terra.common.Column;
import bio.terra.model.IngestRequestModel.FormatEnum;
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

  private final AzureResourceConfiguration azureResourceConfiguration;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private static final String PARSER_VERSION = "2.0";

  @Autowired
  public AzureSynapsePdao(
      AzureResourceConfiguration azureResourceConfiguration,
      AzureBlobStorePdao azureBlobStorePdao) {
    this.azureResourceConfiguration = azureResourceConfiguration;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  public void createExternalDataSource(
      String dataSourceUrl,
      UUID tenantId,
      String scopedCredentialName,
      String dataSourceName,
      SasPermission permissionType)
      throws NotImplementedException, SQLException {

    // parse user provided url to Azure container - can be signed or unsigned
    BlobUrlParts ingestControlFileBlobUrl = BlobUrlParts.parse(dataSourceUrl);
    String blobName = ingestControlFileBlobUrl.getBlobName();

    // during factory build, we check if url is signed
    // if not signed, we generate the sas token
    // when signing, 'tdr' (the Azure app), must be granted permission on the storage account
    // associated with the provided tenant ID
    BlobContainerClientFactory sourceClientFactory =
        azureBlobStorePdao.buildSourceClientFactory(tenantId, ingestControlFileBlobUrl);

    // Given the sas token, rebuild a signed url
    String signedURL = sourceClientFactory.createSasUrlForBlob(blobName, permissionType);
    BlobUrlParts signedBlobUrl = BlobUrlParts.parse(signedURL);
    AzureSasCredential blobContainerSasTokenCreds =
        new AzureSasCredential(signedBlobUrl.getCommonSasQueryParameters().encode());

    //    //Failed attempt to use a SQL template. Uphappy with [] and single quotes around secret
    //    SQLServerDataSource ds = getDatasource();
    //    NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(ds);
    //    String scopedCredentialCreateSQL =
    //        "CREATE DATABASE SCOPED CREDENTIAL [[:scopedCredentialName]\n"
    //            + "WITH IDENTITY = 'SHARED ACCESS SIGNATURE',\n"
    //            + "SECRET = \':secret\';";
    //    MapSqlParameterSource params =
    //        new MapSqlParameterSource()
    //            .addValue("scopedCredentialName", scopedCredentialName)
    //            .addValue("secret", blobContainerSasTokenCreds.getSignature());
    //    int numrows = template.update(scopedCredentialCreateSQL, params);

    executeSynapseQuery(
        "CREATE DATABASE SCOPED CREDENTIAL ["
            + scopedCredentialName
            + "]\n"
            + "WITH IDENTITY = 'SHARED ACCESS SIGNATURE',\n"
            + "SECRET = '"
            + blobContainerSasTokenCreds.getSignature()
            + "';");

    executeSynapseQuery(
        "CREATE EXTERNAL DATA SOURCE "
            + "["
            + dataSourceName
            + "]\n"
            + "WITH (\n"
            + "LOCATION = '"
            + signedBlobUrl.getScheme()
            + "://"
            + signedBlobUrl.getHost()
            + "/"
            + signedBlobUrl.getBlobContainerName()
            + "',\n"
            + "CREDENTIAL = ["
            + scopedCredentialName
            + "]);");
  }

  public void createParquetFiles(
      FormatEnum ingestRequestFormat,
      DatasetTable datasetTable,
      String ingestFileName,
      String destinationParquetFile,
      String destinationDataSourceName,
      String controlFileDataSourceName,
      String ingestTableName,
      int csvSkipLeadingRows)
      throws SQLException {
    executeSynapseQuery(
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
            + azureResourceConfiguration.getSynapse().getParquetFileFormatName()
            + "]\n"
            + ") AS SELECT "
            + buildSelectStatement(ingestRequestFormat, datasetTable)
            + " FROM OPENROWSET("
            + "\n                                "
            + "BULK '"
            + ingestFileName
            + "',\n                                "
            + "DATA_SOURCE = '"
            + controlFileDataSourceName
            + "',\n                                "
            + "FORMAT = 'CSV'"
            + ",\n                                "
            + addArguments(ingestRequestFormat, csvSkipLeadingRows)
            + ")\n"
            + "WITH (\n      "
            + buildWithStatement(ingestRequestFormat, datasetTable)
            + ") AS rows;");
  }

  private String buildSelectStatement(FormatEnum formatType, DatasetTable datasetTable) {
    switch (formatType) {
      case CSV:
        return "*";
      case JSON:
        List<Column> columns = datasetTable.getColumns();
        return String.join(
                ",\n      ",
                columns.stream()
                    .map(c -> translateToJsonConvert(c.getName(), c.getType(), c.isArrayOf()))
                    .collect(Collectors.toList()))
            + "\n";
      default:
        throw new EnumConstantNotPresentException(FormatEnum.class, formatType.name());
    }
  }

  private String buildWithStatement(FormatEnum formatType, DatasetTable datasetTable) {
    switch (formatType) {
      case CSV:
        List<Column> columns = datasetTable.getColumns();
        return String.join(
            ",\n      ",
            columns.stream()
                .map(c -> c.getName() + " " + translateTypeToDdl(c.getType(), c.isArrayOf()))
                .collect(Collectors.toList()));
      case JSON:
        return "doc nvarchar(max)";
      default:
        throw new EnumConstantNotPresentException(FormatEnum.class, formatType.name());
    }
  }

  private String addArguments(FormatEnum formatType, int csvSkipLeadingRows) {
    switch (formatType) {
      case CSV:
        return "PARSER_VERSION = '"
            + PARSER_VERSION
            + "'"
            + ",\n                                "
            + "FIRSTROW = "
            + csvSkipLeadingRows;
      case JSON:
        return "fieldterminator ='0x0b'"
            + ",\n                                "
            + "fieldquote = '0x0b'";
      default:
        throw new EnumConstantNotPresentException(FormatEnum.class, formatType.name());
    }
  }

  public void dropTables(List<String> tableNames) {
    tableNames.stream()
        .forEach(
            tableName -> {
              try {
                executeSynapseQuery("DROP EXTERNAL TABLE [" + tableName + "];");
              } catch (Exception ex) {
                logger.warn("Unable to clean up table {}, ex: {}", tableName, ex.getMessage());
              }
            });
  }

  public void dropDataSources(List<String> dataSourceNames) {
    dataSourceNames.stream()
        .forEach(
            dataSource -> {
              try {
                executeSynapseQuery("DROP EXTERNAL DATA SOURCE [" + dataSource + "];");
              } catch (Exception ex) {
                logger.warn(
                    "Unable to clean up the external data source {}, ex: {}",
                    dataSource,
                    ex.getMessage());
              }
            });
  }

  public void dropScopedCredentials(List<String> credentialNames) {
    credentialNames.stream()
        .forEach(
            credential -> {
              try {
                executeSynapseQuery("DROP DATABASE SCOPED CREDENTIAL [" + credential + "];");
              } catch (Exception ex) {
                logger.warn(
                    "Unable to clean up scoped credential {}, ex: {}", credential, ex.getMessage());
              }
            });
  }

  public boolean executeSynapseQuery(String query) throws SQLException {
    SQLServerDataSource ds = getDatasource();
    try {
      Connection connection = ds.getConnection();
      Statement statement = connection.createStatement();
      return statement.execute(query);
    } catch (SQLException throwables) {
      throw new SQLException("Synapse Query Failed.", throwables);
    }
  }

  private SQLServerDataSource getDatasource() {
    SQLServerDataSource ds = new SQLServerDataSource();
    ds.setServerName(azureResourceConfiguration.getSynapse().getWorkspaceName());
    ds.setUser(azureResourceConfiguration.getSynapse().getSqlAdminUser());
    ds.setPassword(azureResourceConfiguration.getSynapse().getSqlAdminPassword());
    ds.setDatabaseName(azureResourceConfiguration.getSynapse().getDatabaseName());
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

  private String translateToJsonConvert(String name, TableDataType dataType, boolean isArrayOf) {
    if (isArrayOf) {
      return "JSON_VALUE(doc, '$." + name + "') " + name;
    }
    switch (dataType) {
      case TEXT:
      case STRING:
      case DIRREF:
      case FILEREF:
        return "JSON_VALUE(doc, '$." + name + "') " + name;
      default:
        return "cast(JSON_VALUE(doc, '$."
            + name
            + "') as "
            + translateTypeToDdl(dataType, false)
            + ") "
            + name;
    }
  }
}
