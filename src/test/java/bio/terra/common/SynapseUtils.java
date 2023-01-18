package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.ShortUUID;
import com.azure.core.management.Region;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.storage.models.StorageAccount;
import com.azure.storage.blob.BlobUrlParts;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static final String SCRATCH_TABLE_NAME_PREFIX = "scratch_";
  private List<String> tableNames;
  private Map<String, AzureStorageAccountResource> parquetFileNames;
  private List<String> dataSources;
  private List<String> scopedCredentials;
  private List<String> storageAccountIds;
  private AzureResourceManager client;
  private BillingProfileModel billingProfile;
  private AzureApplicationDeploymentResource applicationResource;
  private String snapshotStorageAccountId;
  private String datasetStorageAccountId;
  private BlobUrlParts snapshotSignUrlBlob;
  private String randomFlightId;
  private String snapshotCreateFlightId;

  private String snapshotScopedCredentialName;
  private String snapshotDataSourceName;
  private String sourceDatasetScopedCredentialName;
  private String sourceDatasetDataSourceName;
  private String managedResourceGroupName;

  private AzureStorageAccountResource datasetStorageAccountResource;
  private AzureStorageAccountResource snapshotStorageAccountResource;

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;
  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired JsonLoader jsonLoader;
  @Autowired TestConfiguration testConfiguration;

  public void synapseTestSetup() throws SQLException {
    parquetFileNames = new HashMap<>();
    tableNames = new ArrayList<>();
    dataSources = new ArrayList<>();
    scopedCredentials = new ArrayList<>();
    storageAccountIds = new ArrayList<>();
    UUID applicationId = UUID.randomUUID();
    randomFlightId = ShortUUID.get();
    snapshotCreateFlightId = ShortUUID.get();
    snapshotScopedCredentialName =
        IngestUtils.getTargetScopedCredentialName(snapshotCreateFlightId);
    snapshotDataSourceName = IngestUtils.getTargetDataSourceName(snapshotCreateFlightId);
    addDataSource(snapshotDataSourceName);
    sourceDatasetScopedCredentialName =
        IngestUtils.getSourceDatasetScopedCredentialName(snapshotCreateFlightId);
    sourceDatasetDataSourceName =
        IngestUtils.getSourceDatasetDataSourceName(snapshotCreateFlightId);
    addDataSource(sourceDatasetDataSourceName);
    managedResourceGroupName = testConfig.getAzureManagedResourceGroupName();

    billingProfile =
        new BillingProfileModel()
            .id(UUID.randomUUID())
            .profileName(Names.randomizeName("somename"))
            .biller("direct")
            .billingAccountId(testConfig.getGoogleBillingAccountId())
            .description("random description")
            .cloudPlatform(CloudPlatform.AZURE)
            .tenantId(testConfig.getTargetTenantId())
            .subscriptionId(testConfig.getTargetSubscriptionId())
            .resourceGroupName(testConfig.getTargetResourceGroupName())
            .applicationDeploymentName(testConfig.getTargetApplicationName());

    client =
        azureResourceConfiguration.getClient(
            azureResourceConfiguration.getCredentials().getHomeTenantId(),
            billingProfile.getSubscriptionId());

    applicationResource =
        new AzureApplicationDeploymentResource()
            .id(applicationId)
            .azureApplicationDeploymentName(testConfig.getTargetApplicationName())
            .azureResourceGroupName(managedResourceGroupName)
            .profileId(billingProfile.getId());

    StorageAccount datasetStorageAccount =
        client
            .storageAccounts()
            .define("ctdataset" + Instant.now().toEpochMilli())
            .withRegion(Region.US_CENTRAL)
            .withExistingResourceGroup(managedResourceGroupName)
            .create();
    datasetStorageAccountId = datasetStorageAccount.id();
    datasetStorageAccountResource =
        new AzureStorageAccountResource()
            .resourceId(UUID.randomUUID())
            .name(datasetStorageAccount.name())
            .applicationResource(applicationResource)
            .metadataContainer("metadata");

    StorageAccount snapshotStorageAccount =
        client
            .storageAccounts()
            .define("ctsnapshot" + Instant.now().toEpochMilli())
            .withRegion(Region.US_CENTRAL)
            .withExistingResourceGroup(managedResourceGroupName)
            .create();
    snapshotStorageAccountId = snapshotStorageAccount.id();
    snapshotStorageAccountResource =
        new AzureStorageAccountResource()
            .resourceId(UUID.randomUUID())
            .name(snapshotStorageAccount.name())
            .applicationResource(applicationResource)
            .metadataContainer("metadata");

    // -- CreateSnapshotSourceDatasetDataSourceAzureStep --
    // Create external data source for the source dataset
    // Where we'll pull the dataset data from to then write to the snapshot
    String parquetDatasetSourceLocation = datasetStorageAccountResource.getStorageAccountUrl();
    BlobUrlParts datasetSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetDatasetSourceLocation,
            billingProfile,
            datasetStorageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        datasetSignUrlBlob, sourceDatasetScopedCredentialName, sourceDatasetDataSourceName);

    // -- CreateSnapshotTargetDataSourceAzureStep --
    // Create external data source for the snapshot
    // where we'll write the resulting parquet files
    String parquetSnapshotLocation = snapshotStorageAccountResource.getStorageAccountUrl();
    snapshotSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetSnapshotLocation,
            billingProfile,
            snapshotStorageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        snapshotSignUrlBlob, snapshotScopedCredentialName, snapshotDataSourceName);
  }

  public BlobUrlParts retrieveSnapshotSignUrlBlob() {
    return snapshotSignUrlBlob;
  }

  public AzureStorageAccountResource retrieveDatasetStorageAccountResource() {
    return datasetStorageAccountResource;
  }

  public AzureStorageAccountResource retrieveSnapshotStorageAccountResource() {
    return snapshotStorageAccountResource;
  }

  public BillingProfileModel retrieveBillingProfileModel() {
    return billingProfile;
  }

  public AzureApplicationDeploymentResource retrieveApplicationResource() {
    return applicationResource;
  }

  public String retrieveRandomFlightId() {
    return randomFlightId;
  }

  public String retrieveSourceDatasetDataSourceName() {
    return sourceDatasetDataSourceName;
  }

  public String retrieveSnapshotDataSourceName() {
    return snapshotDataSourceName;
  }

  public void synapseTestCleanup() {
    try {
      azureSynapsePdao.dropTables(tableNames);
    } catch (Exception ex) {
      logger.warn("[Cleanup exception] Unable to drop tables.", ex.getMessage());
    }
    try {
      azureSynapsePdao.dropDataSources(dataSources);
    } catch (Exception ex) {
      logger.warn("[Cleanup exception] Unable to drop data sources", ex.getMessage());
    }
    try {
      azureSynapsePdao.dropScopedCredentials(scopedCredentials);
    } catch (Exception ex) {
      logger.warn("[Cleanup exception] Unable to drop scoped credentials", ex.getMessage());
    }
    try {
      storageAccountIds.forEach(
          storageAccountId -> {
            client.storageAccounts().deleteById(storageAccountId);
          });
    } catch (Exception ex) {
      logger.warn("[Cleanup exception] Unable to delete storage account", ex.getMessage());
    }
    // Parquet File delete is not currently operational
    // To be addressed in DR-2882
    //    for (var parquetFile : parquetFileNames.entrySet()) {
    //      try {
    //        synapseUtils.deleteParquetFile(
    //            billingProfile,
    //            parquetFile.getValue(),
    //            parquetFile.getKey(),
    //            new BlobSasTokenOptions(
    //                Duration.ofMinutes(15),
    //                new BlobSasPermission().setReadPermission(true).setDeletePermission(true),
    //                null));
    //      } catch (Exception ex) {
    //        logger.warn(
    //            "Unable to delete parquet file {}. {}; {}",
    //            parquetFile.getKey(),
    //            ex.getMessage(),
    //            ex.getCause());
    //      }
    //    }
    //
    //      // check to see if successful delete
    //      List<String> emptyList =
    //          synapseUtils.readParquetFileStringColumn(
    //              IngestUtils.getParquetFilePath("all_data_types", randomFlightId),
    //              IngestUtils.getTargetDataSourceName(randomFlightId),
    //              "first_name",
    //              false);
    //      assertThat(
    //          "No longer able to read parquet file because it should have been delete",
    //          emptyList.size(),
    //          equalTo(0));
  }

  public void addParquetFileName(
      String parquetFileName, AzureStorageAccountResource azureStorageAccountResource) {
    parquetFileNames.put(parquetFileName, azureStorageAccountResource);
  }

  public void addTableName(String tableName) {
    tableNames.add(tableName);
  }

  public void addDataSource(String dataSource) {
    dataSources.add(dataSource);
  }

  public void addStorageAccountId(String storageAccountId) {
    storageAccountIds.add(storageAccountId);
  }

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

  public DatasetTable ingestIntoTable(
      String datasetTableSpecFilePath,
      IngestRequestModel ingestRequestModel,
      String ingestFileLocation,
      String flightId,
      String ingestRequestContainer,
      int numRowsToIngest,
      AzureStorageAccountResource datasetStorageAccountResource,
      BillingProfileModel billingProfile)
      throws IOException, SQLException {
    DatasetTable destinationTable =
        jsonLoader.loadObject(datasetTableSpecFilePath, DatasetTable.class);
    destinationTable.id(UUID.randomUUID());
    performIngest(
        destinationTable,
        ingestFileLocation,
        flightId,
        datasetStorageAccountResource,
        billingProfile,
        ingestRequestModel,
        numRowsToIngest,
        ingestRequestContainer);
    addTableName(IngestUtils.getSynapseIngestTableName(flightId));
    addTableName(IngestUtils.getSynapseScratchTableName(flightId));
    return destinationTable;
  }

  public DatasetTable ingestIntoAllDataTypesTable(
      IngestRequestModel ingestRequestModel,
      String ingestFileLocation,
      String randomFlightId,
      AzureStorageAccountResource datasetStorageAccountResource,
      BillingProfileModel billingProfile)
      throws IOException, SQLException {

    DatasetTable destinationTable =
        ingestIntoTable(
            "ingest-test-dataset-table-all-data-types.json",
            ingestRequestModel,
            ingestFileLocation,
            randomFlightId,
            testConfig.getIngestRequestContainer(),
            2,
            datasetStorageAccountResource,
            billingProfile);
    jsonLoader.loadObject("ingest-test-dataset-table-all-data-types.json", DatasetTable.class);

    String scratchParquetFile =
        "parquet/scratch_" + destinationTable.getName() + "/" + randomFlightId + ".parquet";
    addParquetFileName(scratchParquetFile, datasetStorageAccountResource);
    addParquetFileName(
        IngestUtils.getParquetFilePath(destinationTable.getName(), randomFlightId),
        datasetStorageAccountResource);

    // Check that the parquet files were successfully created.
    List<String> firstNames =
        readParquetFileStringColumn(
            IngestUtils.getParquetFilePath(destinationTable.getName(), randomFlightId),
            IngestUtils.getTargetDataSourceName(randomFlightId),
            "first_name",
            true);
    assertThat(
        "List of names should equal the input", firstNames, equalTo(List.of("Bob", "Sally")));
    return destinationTable;
  }
}
