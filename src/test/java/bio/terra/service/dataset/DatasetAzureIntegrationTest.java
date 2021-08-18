package bio.terra.service.dataset;

import static bio.terra.service.filedata.azure.util.BlobIOTestUtility.MIB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.SynapseUtils;
import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.configuration.TestConfiguration.User;
import bio.terra.common.fixtures.Names;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.service.filedata.azure.util.BlobIOTestUtility;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class DatasetAzureIntegrationTest extends UsersBase {
  private static final String omopDatasetName = "it_dataset_omop";
  private static final String omopDatasetDesc =
      "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki";
  private static final String omopDatasetRegionName = AzureRegion.DEFAULT_AZURE_REGION.toString();
  private static final String omopDatasetGcpRegionName =
      GoogleRegion.DEFAULT_GOOGLE_REGION.toString();
  private static Logger logger = LoggerFactory.getLogger(DatasetAzureIntegrationTest.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private AuthService authService;
  @Autowired private TestConfiguration testConfig;
  @Autowired private AzureResourceConfiguration azureResourceConfiguration;
  @Autowired private SynapseUtils synapseUtils;

  private String stewardToken;
  private User steward;
  private UUID datasetId;
  private UUID profileId;
  private BlobIOTestUtility blobIOTestUtility;

  @Before
  public void setup() throws Exception {
    super.setup(false);
    // Voldemort is required by this test since the application is deployed with his user authz'ed
    steward = steward("voldemort");
    stewardToken = authService.getDirectAccessAuthToken(steward.getEmail());
    dataRepoFixtures.resetConfig(steward);
    profileId = dataRepoFixtures.createAzureBillingProfile(steward).getId();
    datasetId = null;
    RequestRetryOptions retryOptions =
        new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            azureResourceConfiguration.getMaxRetries(),
            azureResourceConfiguration.getRetryTimeoutSeconds(),
            null,
            null,
            null);
    blobIOTestUtility =
        new BlobIOTestUtility(
            azureResourceConfiguration.getAppToken(testConfig.getTargetTenantId()),
            testConfig.getSourceStorageAccountName(),
            null,
            retryOptions);
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward);
    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward, datasetId);
      datasetId = null;
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward, profileId);
      profileId = null;
    }
  }

  @Test
  public void datasetsHappyPath() throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "it-dataset-omop.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();

    logger.info("dataset id is " + summaryModel.getId());
    assertThat(summaryModel.getName(), startsWith(omopDatasetName));
    assertThat(summaryModel.getDescription(), equalTo(omopDatasetDesc));

    DatasetModel datasetModel = dataRepoFixtures.getDataset(steward, summaryModel.getId());

    assertThat(datasetModel.getName(), startsWith(omopDatasetName));
    assertThat(datasetModel.getDescription(), equalTo(omopDatasetDesc));

    // There is a delay from when a resource is created in SAM to when it is available in an
    // enumerate call.
    boolean metExpectation =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () -> {
              EnumerateDatasetModel enumerateDatasetModel =
                  dataRepoFixtures.enumerateDatasets(steward);
              boolean found = false;
              for (DatasetSummaryModel oneDataset : enumerateDatasetModel.getItems()) {
                if (oneDataset.getId().equals(datasetModel.getId())) {
                  assertThat(oneDataset.getName(), startsWith(omopDatasetName));
                  assertThat(oneDataset.getDescription(), equalTo(omopDatasetDesc));
                  Map<String, StorageResourceModel> storageMap =
                      datasetModel.getStorage().stream()
                          .collect(
                              Collectors.toMap(
                                  StorageResourceModel::getCloudResource, Function.identity()));

                  AzureRegion omopDatasetRegion = AzureRegion.fromValue(omopDatasetRegionName);
                  GoogleRegion omopDatasetGoogleRegion =
                      GoogleRegion.fromValue(omopDatasetGcpRegionName);
                  assertThat(omopDatasetRegion, notNullValue());
                  assertThat(omopDatasetGoogleRegion, notNullValue());

                  assertThat(
                      "Bucket storage matches",
                      storageMap.entrySet().stream()
                          .filter(e -> e.getKey().equals(GoogleCloudResource.BUCKET.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(
                          omopDatasetGoogleRegion.getRegionOrFallbackBucketRegion().getValue()));

                  assertThat(
                      "Firestore storage matches",
                      storageMap.entrySet().stream()
                          .filter(e -> e.getKey().equals(GoogleCloudResource.FIRESTORE.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(
                          omopDatasetGoogleRegion.getRegionOrFallbackFirestoreRegion().getValue()));

                  assertThat(
                      "Storage account storage matches",
                      storageMap.entrySet().stream()
                          .filter(
                              e -> e.getKey().equals(AzureCloudResource.STORAGE_ACCOUNT.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(omopDatasetRegion.getValue()));

                  found = true;
                  break;
                }
              }
              return found;
            });

    assertTrue("dataset was found in enumeration", metExpectation);

    // This should fail since it currently has dataset storage account within
    assertThrows(AssertionError.class, () -> dataRepoFixtures.deleteProfile(steward, profileId));

    // Create and delete a dataset and make sure that the profile still can't be deleted
    DatasetSummaryModel summaryModel2 =
        dataRepoFixtures.createDataset(steward, profileId, "it-dataset-omop.json");
    dataRepoFixtures.deleteDataset(steward, summaryModel2.getId());
    assertThat(
        "Original dataset is still there",
        dataRepoFixtures
            .getDatasetRaw(steward, summaryModel.getId())
            .getStatusCode()
            .is2xxSuccessful(),
        equalTo(true));
    assertThat(
        "New dataset was deleted",
        dataRepoFixtures.getDatasetRaw(steward, summaryModel2.getId()).getStatusCode().value(),
        // TODO: fix bug where this shows up as a 401 and not a 404 since it's not longer in Sam
        equalTo(401));
    assertThrows(AssertionError.class, () -> dataRepoFixtures.deleteProfile(steward, profileId));

    // Make sure that any failure in tearing down is presented as a test failure
    clearEnvironment();
  }

  @Test
  public void datasetIngestFileHappyPath() throws Exception {
    String blobName = "myBlob";
    long fileSize = MIB / 10;
    String sourceFile = blobIOTestUtility.uploadSourceFile(blobName, fileSize);
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "it-dataset-omop.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                String.format("%s/%s", blobIOTestUtility.getSourceContainerEndpoint(), sourceFile))
            .targetPath("/test/target.txt");
    BulkLoadFileModel fileLoadModelSas =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                String.format(
                    "%s/%s?%s",
                    blobIOTestUtility.getSourceContainerEndpoint(),
                    sourceFile,
                    blobIOTestUtility.generateBlobSasTokenWithReadPermissions(
                        getSourceStorageAccountPrimarySharedKey(), sourceFile)))
            .targetPath("/test/targetSas.txt");
    BulkLoadArrayResultModel result =
        dataRepoFixtures.bulkLoadArray(
            steward,
            datasetId,
            new BulkLoadArrayRequestModel()
                .profileId(summaryModel.getDefaultProfileId())
                .loadTag("loadTag")
                .addLoadArrayItem(fileLoadModel)
                .addLoadArrayItem(fileLoadModelSas));

    assertThat(result.getLoadSummary().getSucceededFiles(), equalTo(2));

    assertThat(
        "file size matches",
        dataRepoFixtures.getFileByName(steward, datasetId, "/test/target.txt").getSize(),
        equalTo(fileSize));

    assertThat(
        "file with Sas size matches",
        dataRepoFixtures.getFileByName(steward, datasetId, "/test/targetSas.txt").getSize(),
        equalTo(fileSize));

    // Ingest Metadata - 1 row from JSON file
    String tableName = "vocabulary";
    String ingestRequestPathJSON =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "azure-vocab-ingest-request.json");
    IngestRequestModel ingestRequestJSON =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(tableName)
            .path(ingestRequestPathJSON)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test"));
    IngestResponseModel ingestResponseJSON =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequestJSON);
    assertThat("1 Row was ingested", ingestResponseJSON.getRowCount(), equalTo(1L));

    // Ingest 2 rows from CSV
    String ingestRequestPathCSV =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "azure-vocab-ingest-request.csv");
    IngestRequestModel ingestRequestCSV =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(tableName)
            .path(ingestRequestPathCSV)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test"))
            .csvSkipLeadingRows(2);
    IngestResponseModel ingestResponseCSV =
        dataRepoFixtures.ingestJsonData(steward(), datasetId, ingestRequestCSV);
    assertThat("2 row were ingested", ingestResponseCSV.getRowCount(), equalTo(2L));

    // Delete the file we just ingested
    String fileId = result.getLoadFileResults().get(0).getFileId();
    dataRepoFixtures.deleteFile(steward, datasetId, fileId);

    assertThat(
        "file is gone",
        dataRepoFixtures.getFileByIdRaw(steward, datasetId, fileId).getStatusCode(),
        equalTo(HttpStatus.NOT_FOUND));

    // Make sure that any failure in tearing down is presented as a test failure
    blobIOTestUtility.deleteContainers();
    clearEnvironment();
  }

  @Test
  public void testDatasetFileIngestLoadHistory() throws Exception {
    String blobName = "myBlob";
    long fileSize = MIB / 10;
    String sourceFile = blobIOTestUtility.uploadSourceFile(blobName, fileSize);
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "it-dataset-omop.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                String.format("%s/%s", blobIOTestUtility.getSourceContainerEndpoint(), sourceFile))
            .targetPath("/test/target.txt");
    BulkLoadFileModel fileLoadModelSas =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                String.format(
                    "%s/%s?%s",
                    blobIOTestUtility.getSourceContainerEndpoint(),
                    sourceFile,
                    blobIOTestUtility.generateBlobSasTokenWithReadPermissions(
                        getSourceStorageAccountPrimarySharedKey(), sourceFile)))
            .targetPath("/test/targetSas.txt");
    BulkLoadArrayResultModel bulkLoadResult1 =
        dataRepoFixtures.bulkLoadArray(
            steward,
            datasetId,
            new BulkLoadArrayRequestModel()
                .profileId(summaryModel.getDefaultProfileId())
                .loadTag("loadTag")
                .addLoadArrayItem(fileLoadModel)
                .addLoadArrayItem(fileLoadModelSas));

    BulkLoadFileModel fileLoadModel2 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                String.format("%s/%s", blobIOTestUtility.getSourceContainerEndpoint(), sourceFile))
            .targetPath("/test/target2.txt");
    BulkLoadFileModel fileLoadModelSas2 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                String.format(
                    "%s/%s?%s",
                    blobIOTestUtility.getSourceContainerEndpoint(),
                    sourceFile,
                    blobIOTestUtility.generateBlobSasTokenWithReadPermissions(
                        getSourceStorageAccountPrimarySharedKey(), sourceFile)))
            .targetPath("/test/targetSas2.txt");

    BulkLoadArrayResultModel bulkLoadResult2 =
        dataRepoFixtures.bulkLoadArray(
            steward,
            datasetId,
            new BulkLoadArrayRequestModel()
                .profileId(summaryModel.getDefaultProfileId())
                .loadTag("loadTag")
                .addLoadArrayItem(fileLoadModel2)
                .addLoadArrayItem(fileLoadModelSas2));

    BulkLoadFileModel fileLoadModel3 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                String.format("%s/%s", blobIOTestUtility.getSourceContainerEndpoint(), sourceFile))
            .targetPath("/test/target3.txt");
    BulkLoadFileModel fileLoadModelSas3 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                String.format(
                    "%s/%s?%s",
                    blobIOTestUtility.getSourceContainerEndpoint(),
                    sourceFile,
                    blobIOTestUtility.generateBlobSasTokenWithReadPermissions(
                        getSourceStorageAccountPrimarySharedKey(), sourceFile)))
            .targetPath("/test/targetSas3.txt");
    dataRepoFixtures.bulkLoadArray(
        steward,
        datasetId,
        new BulkLoadArrayRequestModel()
            .profileId(summaryModel.getDefaultProfileId())
            .loadTag("differentLoadTag")
            .addLoadArrayItem(fileLoadModel3)
            .addLoadArrayItem(fileLoadModelSas3));

    var loadHistoryList1 = dataRepoFixtures.getLoadHistory(steward, datasetId, "loadTag", 0, 2);
    var loadHistoryList2 = dataRepoFixtures.getLoadHistory(steward, datasetId, "loadTag", 2, 10);
    var loadHistoryList1and2 =
        dataRepoFixtures.getLoadHistory(steward, datasetId, "loadTag", 0, 10);
    var loadHistoryList3 =
        dataRepoFixtures.getLoadHistory(steward, datasetId, "differentLoadTag", 0, 10);
    var loaded1and2 =
        Stream.concat(
                bulkLoadResult1.getLoadFileResults().stream(),
                bulkLoadResult2.getLoadFileResults().stream())
            .collect(Collectors.toSet());

    var loadHistory1and2Models =
        Stream.concat(loadHistoryList1.getItems().stream(), loadHistoryList2.getItems().stream());

    assertThat("limited load history is the correct size", loadHistoryList1.getTotal(), equalTo(2));
    assertThat("offset load history is the correct size", loadHistoryList2.getTotal(), equalTo(2));
    assertThat(
        "all load history for load tag is returned", loadHistoryList1and2.getTotal(), equalTo(4));
    assertThat(
        "getting load history has the same items as response from bulk file load",
        loadHistory1and2Models
            .map(TestUtils::toBulkLoadFileResultModel)
            .collect(Collectors.toSet()),
        equalTo(loaded1and2));
    assertThat(
        "load counts under different load tags are returned separately",
        loadHistoryList3.getTotal(),
        equalTo(2));
    for (var loadHistoryModel : loadHistoryList3.getItems()) {
      assertThat(
          "models from different load tags are returned in different requests",
          loadHistoryModel,
          not(in(loadHistoryList1and2.getItems())));
    }

    // Make sure that any failure in tearing down is presented as a test failure
    blobIOTestUtility.deleteContainers();
    clearEnvironment();
  }

  private void clearEnvironment() throws Exception {
    if (datasetId != null) {
      dataRepoFixtures.deleteDataset(steward, datasetId);
      datasetId = null;
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfile(steward, profileId);
      profileId = null;
    }
  }

  private String getSourceStorageAccountPrimarySharedKey() {
    AzureResourceManager client =
        this.azureResourceConfiguration.getClient(
            testConfig.getTargetTenantId(), testConfig.getTargetSubscriptionId());

    return client
        .storageAccounts()
        .getByResourceGroup(
            testConfig.getTargetResourceGroupName(), testConfig.getSourceStorageAccountName())
        .getKeys()
        .iterator()
        .next()
        .value();
  }
}
