package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import com.azure.storage.blob.BlobUrlParts;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
public class SynapseUtils {
  private static final Logger logger = LoggerFactory.getLogger(SynapseUtils.class);
  private static final String READ_FROM_PARQUET_FILE =
      "SELECT [<columnName>] AS [<columnName>]\n"
          + "FROM OPENROWSET(\n"
          + "    BULK '<parquetFilePath>',\n"
          + "    DATA_SOURCE = '<dataSourceName>',\n"
          + "    FORMAT = 'parquet') as rows";
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("SynapseConnectedTest")
          .setEmail("dataset@connected.com")
          .setToken("token")
          .build();
  private static final String INGEST_REQUEST_SCOPED_CREDENTIAL_PREFIX = "irsas_";
  private static final String DESTINATION_SCOPED_CREDENTIAL_PREFIX = "dsas_";
  private static final String SNAPSHOT_SCOPED_CREDENTIAL_PREFIX = "ssas_";
  private static final String INGEST_REQUEST_DATA_SOURCE_PREFIX = "irds_";
  private static final String DESTINATION_DATA_SOURCE_PREFIX = "dds_";
  private static final String SNAPSHOT_DATA_SOURCE_PREFIX = "sds_";
  private static final String TABLE_NAME_PREFIX = "ingest_";
  private static final String SCRATCH_TABLE_NAME_PREFIX = "scratch_";

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired JsonLoader jsonLoader;

  public List<String> readParquetFileStringColumn(
      String parquetFilePath, String dataSourceName, String columnName, boolean expectSuccess) {
    ST sqlReadTemplate = new ST(READ_FROM_PARQUET_FILE);
    sqlReadTemplate.add("parquetFilePath", parquetFilePath);
    sqlReadTemplate.add("dataSourceName", dataSourceName);
    sqlReadTemplate.add("columnName", columnName);
    SQLServerDataSource ds = azureSynapsePdao.getDatasource();
    List<String> resultList = new ArrayList<>();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sqlReadTemplate.render())) {
      while (rs.next()) {
        resultList.add(rs.getString(columnName));
      }
    } catch (SQLException ex) {
      if (expectSuccess) {
        logger.error("Unable to read parquet file.", ex);
      }
    }
    return resultList;
  }

  public void deleteParquetFile(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccount,
      String parquetFileName,
      BlobSasTokenOptions blobSasTokenOptions) {
    BlobContainerClientFactory targetDataClientFactory =
        azureBlobStorePdao.getTargetDataClientFactory(
            profileModel,
            storageAccount,
            AzureStorageAccountResource.ContainerType.METADATA,
            blobSasTokenOptions);

    var result =
        targetDataClientFactory
            .getBlobContainerClient()
            .listBlobsByHierarchy(parquetFileName + "/");
    result.forEach(
        s -> {
          if (s.isPrefix() == null || !s.isPrefix()) {
            logger.info("Attempting to delete the parquet blob: {}", s.getName());
            targetDataClientFactory.getBlobContainerClient().getBlobClient(s.getName()).delete();
          }
        });
    logger.info("Attempting to delete the parquet directory {}", parquetFileName);
    targetDataClientFactory.getBlobContainerClient().getBlobClient(parquetFileName).delete();
  }

  public String ingestRequestURL(
      String storageAccountName, String ingestRequestContainer, String fileName) {
    String sqlTemplate =
        "https://<storageAccountName>.blob.core.windows.net/<ingestRequestContainer>/<requestFileName>";
    ST urlTemplate = new ST(sqlTemplate);
    urlTemplate.add("storageAccountName", storageAccountName);
    urlTemplate.add("ingestRequestContainer", ingestRequestContainer);
    urlTemplate.add("requestFileName", fileName);
    return urlTemplate.render();
  }

  public void performIngest(
      DatasetTable destinationTable,
      String ingestFilePath,
      String ingestFlightId,
      AzureStorageAccountResource storageAccountResource,
      BillingProfileModel billingProfile,
      IngestRequestModel ingestRequestModel,
      int numRowsToIngest,
      String ingestRequestContainer)
      throws SQLException, IOException {
    UUID tenantId = testConfig.getTargetTenantId();
    String ingestFileLocation =
        ingestRequestURL(
            testConfig.getSourceStorageAccountName(), ingestRequestContainer, ingestFilePath);

    // ---- ingest steps ---
    // 0 -
    // A - Collect user input and validate
    IngestUtils.validateBlobAzureBlobFileURL(ingestFileLocation);

    // B - Build parameters based on user input
    // Moved to lower in this method

    // 1 - Create external data source for the ingest control file
    BlobUrlParts ingestRequestSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForSourceFactory(ingestFileLocation, tenantId, TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        ingestRequestSignUrlBlob,
        IngestUtils.getIngestRequestScopedCredentialName(ingestFlightId),
        IngestUtils.getIngestRequestDataSourceName(ingestFlightId));

    // 2 - Create the external data source for the destination
    // where we'll write the resulting parquet files
    // We will build this parquetDestinationLocation according
    // to the associated storage account for the dataset
    String parquetDestinationLocation = storageAccountResource.getStorageAccountUrl();

    BlobUrlParts destinationSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetDestinationLocation,
            billingProfile,
            storageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        destinationSignUrlBlob,
        IngestUtils.getTargetScopedCredentialName(ingestFlightId),
        IngestUtils.getTargetDataSourceName(ingestFlightId));

    // 3 - Retrieve info about database schema so that we can populate the parquet create query
    String tableName = destinationTable.getName();
    String destinationParquetFile = "parquet/" + tableName + "/" + ingestFlightId + ".parquet";

    String scratchParquetFile =
        "parquet/" + SCRATCH_TABLE_NAME_PREFIX + tableName + "/" + ingestFlightId + ".parquet";

    // 4 - Create parquet files via external table
    // All inputs should be sanitized before passed into this method
    int updateCount =
        azureSynapsePdao.createScratchParquetFiles(
            ingestRequestModel.getFormat(),
            destinationTable,
            ingestRequestSignUrlBlob.getBlobName(),
            scratchParquetFile,
            IngestUtils.getTargetDataSourceName(ingestFlightId),
            IngestUtils.getIngestRequestDataSourceName(ingestFlightId),
            IngestUtils.getSynapseScratchTableName(ingestFlightId),
            ingestRequestModel.getCsvSkipLeadingRows(),
            ingestRequestModel.getCsvFieldDelimiter(),
            ingestRequestModel.getCsvQuote());
    assertThat("num rows updated is two", updateCount, equalTo(numRowsToIngest));

    int failedRows =
        azureSynapsePdao.validateScratchParquetFiles(
            destinationTable, IngestUtils.getSynapseScratchTableName(ingestFlightId));

    assertThat("there are no rows that fail validation", failedRows, equalTo(0));

    azureSynapsePdao.createFinalParquetFiles(
        IngestUtils.getSynapseIngestTableName(ingestFlightId),
        destinationParquetFile,
        IngestUtils.getDataSourceName(
            AzureStorageAccountResource.ContainerType.METADATA, ingestFlightId),
        IngestUtils.getSynapseScratchTableName(ingestFlightId),
        destinationTable);
  }
}
