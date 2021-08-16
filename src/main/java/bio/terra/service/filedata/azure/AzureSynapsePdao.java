package bio.terra.service.filedata.azure;

import bio.terra.common.Column;
import bio.terra.common.SynapseColumn;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.job.exception.InvalidJobParameterException;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
public class AzureSynapsePdao {
  private static final Logger logger = LoggerFactory.getLogger(AzureSynapsePdao.class);

  private final AzureResourceConfiguration azureResourceConfiguration;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private static final String PARSER_VERSION = "2.0";
  private static final Duration DEFAULT_SAS_TOKEN_EXPIRATION = Duration.ofHours(24);
  private static final String scopedCredentialCreateTemplate =
      "CREATE DATABASE SCOPED CREDENTIAL [<scopedCredentialName>]\n"
          + "WITH IDENTITY = 'SHARED ACCESS SIGNATURE',\n"
          + "SECRET = '<secret>';";

  private static final String dataSourceCreateTemplate =
      "CREATE EXTERNAL DATA SOURCE [<dataSourceName>]\n"
          + "WITH (\n"
          + "    LOCATION = '<scheme>://<host>/<container>',\n"
          + "    CREDENTIAL = [<credential>]\n"
          + ");";

  private static final String createTableTemplate =
      "CREATE EXTERNAL TABLE [<tableName>]\n"
          + "WITH (\n"
          + "    LOCATION = '<destinationParquetFile>',\n"
          + "    DATA_SOURCE = [<destinationDataSourceName>],\n"
          + "    FILE_FORMAT = [<fileFormat>]\n"
          + ") AS SELECT "
          + "<if(isCSV)>*"
          + "<else>"
          + "\n<columns:{c|"
          + "<if(c.requiresJSONCast)>"
          + "cast(JSON_VALUE(doc, '$.<c.name>') as <c.synapseDataType>) <c.name>"
          + "<else>JSON_VALUE(doc, '$.<c.name>') <c.name>"
          + "<endif>"
          + "}; separator=\",\n       \">\n"
          + "<endif>"
          + " FROM\n"
          + "    OPENROWSET(\n"
          + "       BULK '<ingestFileName>',\n"
          + "       DATA_SOURCE = '<controlFileDataSourceName>',\n"
          + "       FORMAT = 'CSV',\n"
          + "<if(isCSV)>"
          + "       PARSER_VERSION = '<parserVersion>',\n"
          + "       FIRSTROW = <firstRow>\n"
          + "<else>"
          + "       fieldterminator ='0x0b',\n"
          + "       fieldquote = '0x0b'\n"
          + "<endif>"
          + "    ) WITH (\n"
          + "      <if(isCSV)>"
          + "<columns:{c|<c.name> <c.synapseDataType>"
          + "<if(c.requiresCollate)> COLLATE Latin1_General_100_CI_AI_SC_UTF8<endif>"
          + "}; separator=\",\n\">"
          + "<else>doc nvarchar(max)"
          + "<endif>\n"
          + ") AS rows;";

  private static final String dropTableTemplate = "DROP EXTERNAL TABLE [<resourceName>];";

  private static final String dropDataSourceTemplate =
      "DROP EXTERNAL DATA SOURCE [<resourceName>];";

  private static final String dropScopedCredentialTemplate =
      "DROP DATABASE SCOPED CREDENTIAL [<resourceName>];";

  @Autowired
  public AzureSynapsePdao(
      AzureResourceConfiguration azureResourceConfiguration,
      AzureBlobStorePdao azureBlobStorePdao) {
    this.azureResourceConfiguration = azureResourceConfiguration;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  public BlobUrlParts getOrSignUrlForSourceFactory(String dataSourceUrl, UUID tenantId) {
    // parse user provided url to Azure container - can be signed or unsigned
    BlobUrlParts ingestControlFileBlobUrl = BlobUrlParts.parse(dataSourceUrl);
    String blobName = ingestControlFileBlobUrl.getBlobName();

    // during factory build, we check if url is signed
    // if not signed, we generate the sas token
    // when signing, 'tdr' (the Azure app), must be granted permission on the storage account
    // associated with the provided tenant ID
    BlobContainerClientFactory sourceClientFactory =
        azureBlobStorePdao.buildSourceClientFactory(tenantId, dataSourceUrl);

    // Given the sas token, rebuild a signed url
    BlobSasTokenOptions options =
        new BlobSasTokenOptions(
            DEFAULT_SAS_TOKEN_EXPIRATION,
            new BlobSasPermission().setReadPermission(true),
            AzureSynapsePdao.class.getName());
    String signedURL =
        sourceClientFactory.getBlobSasUrlFactory().createSasUrlForBlob(blobName, options);
    return BlobUrlParts.parse(signedURL);
  }

  public BlobUrlParts getOrSignUrlForTargetFactory(
      String dataSourceUrl,
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccount) {
    BlobUrlParts ingestControlFileBlobUrl = BlobUrlParts.parse(dataSourceUrl);
    String blobName = ingestControlFileBlobUrl.getBlobName();

    BlobContainerClientFactory targetDataClientFactory =
        azureBlobStorePdao.getTargetDataClientFactory(
            profileModel, storageAccount, ContainerType.METADATA, false);

    // Given the sas token, rebuild a signed url
    BlobSasTokenOptions options =
        new BlobSasTokenOptions(
            DEFAULT_SAS_TOKEN_EXPIRATION,
            new BlobSasPermission()
                .setReadPermission(true)
                .setListPermission(true)
                .setWritePermission(true),
            AzureSynapsePdao.class.getName());
    String signedURL =
        targetDataClientFactory.getBlobSasUrlFactory().createSasUrlForBlob(blobName, options);
    return BlobUrlParts.parse(signedURL);
  }

  public void createExternalDataSource(
      BlobUrlParts signedBlobUrl, String scopedCredentialName, String dataSourceName)
      throws NotImplementedException, SQLException {
    AzureSasCredential blobContainerSasTokenCreds =
        new AzureSasCredential(signedBlobUrl.getCommonSasQueryParameters().encode());

    ST sqlScopedCredentialCreateTemplate = new ST(scopedCredentialCreateTemplate);
    sqlScopedCredentialCreateTemplate.add("scopedCredentialName", scopedCredentialName);
    sqlScopedCredentialCreateTemplate.add("secret", blobContainerSasTokenCreds.getSignature());
    executeSynapseQuery(sqlScopedCredentialCreateTemplate.render());

    ST sqlDataSourceCreateTemplate = new ST(dataSourceCreateTemplate);
    sqlDataSourceCreateTemplate.add("dataSourceName", dataSourceName);
    sqlDataSourceCreateTemplate.add("scheme", signedBlobUrl.getScheme());
    sqlDataSourceCreateTemplate.add("host", signedBlobUrl.getHost());
    sqlDataSourceCreateTemplate.add("container", signedBlobUrl.getBlobContainerName());
    sqlDataSourceCreateTemplate.add("credential", scopedCredentialName);
    executeSynapseQuery(sqlDataSourceCreateTemplate.render());
  }

  public int createParquetFiles(
      FormatEnum ingestType,
      DatasetTable datasetTable,
      String ingestFileName,
      String destinationParquetFile,
      String destinationDataSourceName,
      String controlFileDataSourceName,
      String ingestTableName,
      Optional<Integer> csvSkipLeadingRows)
      throws SQLException {

    List<SynapseColumn> columns =
        datasetTable.getColumns().stream()
            .map(Column::toSynapseColumn)
            .collect(Collectors.toList());
    boolean isCSV = ingestType == FormatEnum.CSV;

    ST sqlCreateTableTemplate = new ST(createTableTemplate);
    sqlCreateTableTemplate.add("isCSV", isCSV);
    if (isCSV) {
      sqlCreateTableTemplate.add("parserVersion", PARSER_VERSION);
      if (csvSkipLeadingRows.isEmpty()) {
        throw new InvalidJobParameterException(
            "For CSV ingests, 'csvSkipLeadingRows' must be defined.");
      }
      sqlCreateTableTemplate.add("firstRow", csvSkipLeadingRows.get());
    }
    sqlCreateTableTemplate.add("tableName", ingestTableName);
    sqlCreateTableTemplate.add("destinationParquetFile", destinationParquetFile);
    sqlCreateTableTemplate.add("destinationDataSourceName", destinationDataSourceName);
    sqlCreateTableTemplate.add(
        "fileFormat", azureResourceConfiguration.getSynapse().getParquetFileFormatName());
    sqlCreateTableTemplate.add("ingestFileName", ingestFileName);
    sqlCreateTableTemplate.add("controlFileDataSourceName", controlFileDataSourceName);
    sqlCreateTableTemplate.add("columns", columns);

    return executeSynapseQuery(sqlCreateTableTemplate.render());
  }

  public void dropTables(List<String> tableNames) {
    cleanup(tableNames, dropTableTemplate);
  }

  public void dropDataSources(List<String> dataSourceNames) {
    cleanup(dataSourceNames, dropDataSourceTemplate);
  }

  public void dropScopedCredentials(List<String> credentialNames) {
    cleanup(credentialNames, dropScopedCredentialTemplate);
  }

  private void cleanup(List<String> resourceNames, String sql) {
    resourceNames.stream()
        .forEach(
            resource -> {
              try {
                ST sqlTemplate = new ST(sql);
                sqlTemplate.add("resourceName", resource);
                executeSynapseQuery(sqlTemplate.render());
              } catch (Exception ex) {
                logger.warn("Unable to clean up synapse resource {}.", resource, ex);
              }
            });
  }

  public int executeSynapseQuery(String query) throws SQLException {
    SQLServerDataSource ds = getDatasource();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(query);
      return statement.getUpdateCount();
    }
  }

  @VisibleForTesting
  public SQLServerDataSource getDatasource() {
    SQLServerDataSource ds = new SQLServerDataSource();
    ds.setServerName(azureResourceConfiguration.getSynapse().getWorkspaceName());
    ds.setUser(azureResourceConfiguration.getSynapse().getSqlAdminUser());
    ds.setPassword(azureResourceConfiguration.getSynapse().getSqlAdminPassword());
    ds.setDatabaseName(azureResourceConfiguration.getSynapse().getDatabaseName());
    return ds;
  }
}
