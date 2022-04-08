package bio.terra.service.dataset;

import static bio.terra.service.filedata.azure.util.BlobIOTestUtility.MIB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.ParquetUtils;
import bio.terra.common.SynapseUtils;
import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.configuration.TestConfiguration.User;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.AccessInfoParquetModelTable;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadHistoryModelList;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DRSAccessMethod.TypeEnum;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsResponse;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobIOTestUtility;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.fasterxml.jackson.core.type.TypeReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
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
import org.springframework.util.ResourceUtils;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class DatasetAzureIntegrationTest extends UsersBase {

  private static final String omopDatasetName = "it_dataset_omop";
  private static final String omopDatasetDesc =
      "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki with extra columns suffixed with _custom";
  private static final String omopDatasetRegionName = AzureRegion.DEFAULT_AZURE_REGION.toString();
  private static final String omopDatasetGcpRegionName =
      GoogleRegion.DEFAULT_GOOGLE_REGION.toString();
  private static Logger logger = LoggerFactory.getLogger(DatasetAzureIntegrationTest.class);

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private AuthService authService;
  @Autowired private TestConfiguration testConfig;
  @Autowired private AzureResourceConfiguration azureResourceConfiguration;
  @Autowired private SynapseUtils synapseUtils;
  @Autowired private JsonLoader jsonLoader;
  @Rule @Autowired public TestJobWatcher testWatcher;

  private String stewardToken;
  private User steward;
  private UUID datasetId;
  private UUID snapshotId;
  private UUID snapshotByRowId;
  private UUID profileId;
  private BlobIOTestUtility blobIOTestUtility;
  private RequestRetryOptions retryOptions;

  @Before
  public void setup() throws Exception {
    super.setup(false);
    // Voldemort is required by this test since the application is deployed with his user authz'ed
    steward = steward("voldemort");
    stewardToken = authService.getDirectAccessAuthToken(steward.getEmail());
    dataRepoFixtures.resetConfig(steward);
    profileId = dataRepoFixtures.createAzureBillingProfile(steward).getId();
    datasetId = null;
    retryOptions =
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
    if (snapshotId != null) {
      dataRepoFixtures.deleteSnapshot(steward, snapshotId);
      snapshotId = null;
    }
    if (snapshotByRowId != null) {
      dataRepoFixtures.deleteSnapshot(steward, snapshotByRowId);
      snapshotId = null;
    }
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
                  assertThat(omopDatasetRegion, notNullValue());

                  assertThat(
                      "Bucket storage matches",
                      storageMap.entrySet().stream()
                          .filter(
                              e ->
                                  e.getKey()
                                      .equals(AzureCloudResource.APPLICATION_DEPLOYMENT.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(omopDatasetRegion.getValue()));

                  assertThat(
                      "Firestore storage matches",
                      storageMap.entrySet().stream()
                          .filter(
                              e ->
                                  e.getKey()
                                      .equals(AzureCloudResource.SYNAPSE_WORKSPACE.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(omopDatasetRegion.getValue()));

                  assertThat(
                      "Storage account storage matches",
                      storageMap.entrySet().stream()
                          .filter(
                              e -> e.getKey().equals(AzureCloudResource.STORAGE_ACCOUNT.getValue()))
                          .findAny()
                          .map(e -> e.getValue().getRegion())
                          .orElseThrow(() -> new RuntimeException("Key not found")),
                      equalTo(omopDatasetRegion.getValue()));

                  assertThat(
                      "dataset summary has Azure cloud platform",
                      oneDataset.getCloudPlatform(),
                      equalTo(CloudPlatform.AZURE));

                  assertThat(
                      "dataset summary has storage account",
                      oneDataset.getStorageAccount(),
                      notNullValue());

                  assertThat(
                      "No Google storage resources are included",
                      storageMap.values().stream()
                          .map(StorageResourceModel::getCloudResource)
                          .collect(Collectors.toSet()),
                      equalTo(
                          Set.of(
                              AzureCloudResource.STORAGE_ACCOUNT.getValue(),
                              AzureCloudResource.SYNAPSE_WORKSPACE.getValue(),
                              AzureCloudResource.APPLICATION_DEPLOYMENT.getValue())));

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
        dataRepoFixtures.createDataset(
            steward, profileId, "it-dataset-omop.json", CloudPlatform.AZURE);
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
            .sourcePath(String.format(blobIOTestUtility.createSourcePath(sourceFile)))
            .targetPath("/test/target.txt");
    BulkLoadFileModel fileLoadModelAlt1 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(String.format(blobIOTestUtility.createSourcePath(sourceFile)))
            .targetPath("/test/target_alt1.txt");
    BulkLoadFileModel fileLoadModelSas =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                blobIOTestUtility.createSourceSignedPath(
                    sourceFile, getSourceStorageAccountPrimarySharedKey()))
            .targetPath("/test/targetSas.txt");
    BulkLoadFileModel fileLoadModelAlt2 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(String.format(blobIOTestUtility.createSourcePath(sourceFile)))
            .targetPath("/test/target_alt2.txt");
    BulkLoadArrayResultModel result =
        dataRepoFixtures.bulkLoadArray(
            steward,
            datasetId,
            new BulkLoadArrayRequestModel()
                .profileId(summaryModel.getDefaultProfileId())
                .loadTag("loadTag")
                .addLoadArrayItem(fileLoadModel)
                .addLoadArrayItem(fileLoadModelAlt1)
                .addLoadArrayItem(fileLoadModelSas)
                .addLoadArrayItem(fileLoadModelAlt2));

    assertThat(result.getLoadSummary().getSucceededFiles(), equalTo(4));

    assertThat(
        "file size matches",
        dataRepoFixtures.getFileByName(steward, datasetId, "/test/target.txt").getSize(),
        equalTo(fileSize));

    assertThat(
        "file with Sas size matches",
        dataRepoFixtures.getFileByName(steward, datasetId, "/test/targetSas.txt").getSize(),
        equalTo(fileSize));

    // lookup file
    List<BulkLoadFileResultModel> loadedFiles = result.getLoadFileResults();
    BulkLoadFileResultModel file1 = loadedFiles.get(0);
    FileModel file1Model = dataRepoFixtures.getFileById(steward(), datasetId, file1.getFileId());
    assertThat("Test retrieve file by ID", file1Model.getFileId(), equalTo(file1.getFileId()));

    FileModel file2Model =
        dataRepoFixtures.getFileById(steward(), datasetId, loadedFiles.get(1).getFileId());

    BulkLoadFileResultModel file3 = loadedFiles.get(2);
    FileModel file3Model =
        dataRepoFixtures.getFileByName(steward(), datasetId, file3.getTargetPath());
    assertThat("Test retrieve file by path", file3Model.getFileId(), equalTo(file3.getFileId()));

    FileModel file4Model =
        dataRepoFixtures.getFileById(steward(), datasetId, loadedFiles.get(3).getFileId());

    // ingest via control file
    String flightId = UUID.randomUUID().toString();
    String controlFileBlob = flightId + "/file-ingest-request.json";
    List<BulkLoadFileModel> bulkLoadFileModelList = new ArrayList<>();
    bulkLoadFileModelList.add(
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(blobIOTestUtility.createSourcePath(sourceFile))
            .targetPath(String.format("/%s/%s", flightId, "target.txt")));
    bulkLoadFileModelList.add(
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(blobIOTestUtility.createSourcePath(sourceFile))
            .targetPath(String.format("/%s/%s", flightId, "target2.txt")));

    String controlFileUrl =
        blobIOTestUtility.uploadFileWithContents(
            controlFileBlob, TestUtils.readControlFile(bulkLoadFileModelList));

    String bulkLoadTag = Names.randomizeName("loadTag");
    BulkLoadRequestModel request =
        new BulkLoadRequestModel()
            .loadControlFile(controlFileUrl)
            .loadTag(bulkLoadTag)
            .profileId(profileId);
    BulkLoadResultModel bulkLoadResult = dataRepoFixtures.bulkLoad(steward, datasetId, request);
    assertThat("result", bulkLoadResult.getSucceededFiles(), equalTo(2));

    // Control file test - Look up the loaded files
    BulkLoadHistoryModelList controlFileLoadResults =
        dataRepoFixtures.getLoadHistory(steward, datasetId, bulkLoadTag, 0, 2);
    for (BulkLoadHistoryModel bulkFileEntry : controlFileLoadResults.getItems()) {
      assertNotNull(dataRepoFixtures.getFileById(steward(), datasetId, bulkFileEntry.getFileId()));
    }

    // dataset ingest
    // Ingest Metadata - 1 row from JSON file
    String datasetIngestFlightId = UUID.randomUUID().toString();
    String datasetIngestControlFileBlob =
        datasetIngestFlightId + "/azure-domain-ingest-request.json";
    Map<String, Object> domainRowData =
        Map.ofEntries(
            Map.entry("domain_id", "1"),
            Map.entry("domain_name", "domain1"),
            Map.entry("domain_concept_id", 1),
            Map.entry("domain_array_tags_custom", List.of("tag1", "tag2")),
            Map.entry(
                "domain_files_custom_1", List.of(file1Model.getFileId(), file3Model.getFileId())),
            Map.entry("domain_files_custom_2", List.of(file2Model.getFileId())),
            Map.entry("domain_files_custom_3", file4Model.getFileId()));
    String ingestRequestPathJSON =
        blobIOTestUtility.uploadFileWithContents(
            datasetIngestControlFileBlob,
            Objects.requireNonNull(TestUtils.mapToJson(domainRowData)));

    String jsonIngestTableName = "domain";
    IngestRequestModel ingestRequestJSON =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.JSON)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(jsonIngestTableName)
            .path(ingestRequestPathJSON)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test"));
    IngestResponseModel ingestResponseJSON =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequestJSON);
    assertThat("1 row was ingested", ingestResponseJSON.getRowCount(), equalTo(1L));

    // Ingest 2 rows from CSV
    String ingest2TableName = "vocabulary";
    String csvDatasetIngestFlightId = UUID.randomUUID().toString();
    String csvDatasetIngestControlFileBlob =
        csvDatasetIngestFlightId + "/azure-vocab-ingest-request.csv";
    String ingestRequestPathCSV =
        blobIOTestUtility.uploadFileWithContents(
            csvDatasetIngestControlFileBlob,
            String.format(
                "vocabulary_id,vocabulary_name,vocabulary_reference,vocabulary_version,vocabulary_concept_id%n"
                    + "\"1\",\"vocab1\",\"%s\",\"v1\",1%n"
                    + "\"2\",\"vocab2\",\"%s\",\"v2\",2",
                file1Model.getFileId(), file3Model.getFileId()));
    IngestRequestModel ingestRequestCSV =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table(ingest2TableName)
            .path(ingestRequestPathCSV)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test"))
            .csvSkipLeadingRows(2);
    IngestResponseModel ingestResponseCSV =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequestCSV);
    assertThat("2 row were ingested", ingestResponseCSV.getRowCount(), equalTo(2L));

    // Only check the subset of tables that have rows
    Set<String> tablesToCheck = Set.of(jsonIngestTableName, ingest2TableName);
    // Read the ingested metadata

    DatasetModel datasetModel =
        dataRepoFixtures.getDataset(
            steward(),
            datasetId,
            List.of(
                DatasetRequestAccessIncludeModel.ACCESS_INFORMATION,
                DatasetRequestAccessIncludeModel.SCHEMA));

    AccessInfoParquetModel datasetParquetAccessInfo =
        datasetModel.getAccessInformation().getParquet();

    DatasetSpecificationModel datasetSchema = datasetModel.getSchema();

    // Create snapshot request for snapshot by row id
    String datasetParquetUrl =
        datasetParquetAccessInfo.getUrl() + "?" + datasetParquetAccessInfo.getSasToken();
    TestUtils.verifyHttpAccess(datasetParquetUrl, Map.of());
    verifySignedUrl(datasetParquetUrl, steward(), "rl");

    SnapshotRequestModel snapshotByRowIdModel = new SnapshotRequestModel();
    snapshotByRowIdModel.setName("row_id_test");
    snapshotByRowIdModel.setDescription("snapshot by row id test");

    SnapshotRequestContentsModel contentsModel = new SnapshotRequestContentsModel();
    contentsModel.setDatasetName(summaryModel.getName());
    contentsModel.setMode(SnapshotRequestContentsModel.ModeEnum.BYROWID);

    SnapshotRequestRowIdModel snapshotRequestRowIdModel = new SnapshotRequestRowIdModel();

    for (AccessInfoParquetModelTable table : datasetParquetAccessInfo.getTables()) {
      if (tablesToCheck.contains(table.getName())) {
        String tableUrl = table.getUrl() + "?" + table.getSasToken();
        TestUtils.verifyHttpAccess(tableUrl, Map.of());
        verifySignedUrl(tableUrl, steward(), "rl");

        BlobContainerClientFactory fact = new BlobContainerClientFactory(tableUrl, retryOptions);

        List<BlobItem> blobItems =
            fact
                .getBlobContainerClient()
                .listBlobsByHierarchy(String.format("parquet/%s/", table.getName()))
                .stream()
                .collect(Collectors.toList());

        List<UUID> rowIds = new ArrayList<>();
        SnapshotRequestRowIdTableModel tableModel = new SnapshotRequestRowIdTableModel();
        tableModel.setTableName(table.getName());
        tableModel.setColumns(
            datasetSchema.getTables().stream()
                .filter(t -> t.getName().equals(table.getName()))
                .flatMap(t -> t.getColumns().stream().map(c -> c.getName()))
                .collect(Collectors.toList()));

        // for each ingest in the dataset, read the associated parquet file
        // in this test, should only be one
        blobItems.stream()
            .forEach(
                item -> {
                  BlobUrlParts url = BlobUrlParts.parse(table.getUrl());
                  String container = item.getName();
                  url.setBlobName(container);
                  String newUrl = url.toUrl() + "?" + table.getSasToken();

                  List<Map<String, String>> records = ParquetUtils.readParquetRecords(newUrl);
                  records.stream()
                      .map(r -> r.get("datarepo_row_id"))
                      .forEach(
                          rowId -> {
                            rowIds.add(UUID.fromString(rowId));
                          });
                });
        tableModel.setRowIds(rowIds);
        snapshotRequestRowIdModel.addTablesItem(tableModel);
      }
    }

    contentsModel.setRowIdSpec(snapshotRequestRowIdModel);
    snapshotByRowIdModel.setContents(List.of(contentsModel));

    // Create Snapshot by full view
    SnapshotRequestModel requestModelAll =
        jsonLoader.loadObject("ingest-test-snapshot-fullviews.json", SnapshotRequestModel.class);
    requestModelAll.getContents().get(0).datasetName(summaryModel.getName());

    SnapshotSummaryModel snapshotSummaryAll =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), summaryModel.getName(), profileId, requestModelAll);
    snapshotId = snapshotSummaryAll.getId();
    assertThat("Snapshot exists", snapshotSummaryAll.getName(), equalTo(requestModelAll.getName()));

    // Read the ingested metadata
    AccessInfoParquetModel snapshotParquetAccessInfo =
        dataRepoFixtures
            .getSnapshot(
                steward(), snapshotId, List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION))
            .getAccessInformation()
            .getParquet();

    String snapshotParquetUrl =
        snapshotParquetAccessInfo.getUrl() + "?" + snapshotParquetAccessInfo.getSasToken();
    TestUtils.verifyHttpAccess(snapshotParquetUrl, Map.of());
    verifySignedUrl(snapshotParquetUrl, steward(), "rl");

    List<String> drsIds = new ArrayList<>();
    for (AccessInfoParquetModelTable table : snapshotParquetAccessInfo.getTables()) {
      if (tablesToCheck.contains(table.getName())) {
        String tableUrl = table.getUrl() + "?" + table.getSasToken();
        TestUtils.verifyHttpAccess(tableUrl, Map.of());
        verifySignedUrl(tableUrl, steward(), "rl");

        // The vocabulary table has file data so test Drs on that one
        // TODO: once we have an endpoint to expose parquet data, we should use that mechanism here
        if (table.getName().equals("vocabulary")) {
          List<Map<String, String>> records = ParquetUtils.readParquetRecords(tableUrl);
          assertThat("2 rows are present", records, hasSize(2));

          // Extract the DRS Ids
          records.stream().map(r -> r.get("vocabulary_reference")).forEach(drsIds::add);
        }
      }
    }

    AccessInfoParquetModelTable domainTable =
        snapshotParquetAccessInfo.getTables().stream()
            .filter(t -> t.getName().equals("domain"))
            .findAny()
            .orElseThrow();

    String domainTableUrl = domainTable.getUrl() + "?" + domainTable.getSasToken();
    List<Map<String, String>> records = ParquetUtils.readParquetRecords(domainTableUrl);
    assertThat("1 row is present", records, hasSize(1));
    assertThat(
        "record looks as expected - domain_id",
        records.get(0).get("domain_id"),
        equalTo(domainRowData.get("domain_id")));
    assertThat(
        "record looks as expected - domain_name",
        records.get(0).get("domain_name"),
        equalTo(domainRowData.get("domain_name")));
    assertThat(
        "record looks as expected - domain_concept_id",
        records.get(0).get("domain_concept_id"),
        equalTo(domainRowData.get("domain_concept_id").toString()));
    assertThat(
        "record looks as expected - domain_array_tags_custom",
        records.get(0).get("domain_array_tags_custom"),
        equalTo("[\"tag1\",\"tag2\"]"));
    List<String> embeddedDrsIds1 =
        TestUtils.mapFromJson(
            records.get(0).get("domain_files_custom_1"), new TypeReference<>() {});
    assertThat(
        "record looks as expected - domain_files_custom_1 drs ids",
        embeddedDrsIds1,
        containsInAnyOrder(drsIds.toArray()));
    List<String> embeddedDrsIds2 =
        TestUtils.mapFromJson(
            records.get(0).get("domain_files_custom_2"), new TypeReference<>() {});
    assertThat(
        "record looks as expected - domain_files_custom_2 drs ids - size",
        embeddedDrsIds2,
        hasSize(1));
    assertThat(
        "record looks as expected - domain_files_custom_2 drs ids - value",
        DrsIdService.fromUri(embeddedDrsIds2.get(0)).toDrsObjectId(),
        equalTo(String.format("v1_%s_%s", snapshotId, file2Model.getFileId())));
    assertThat(
        "record looks as expected - domain_files_custom_3 drs id",
        DrsIdService.fromUri(records.get(0).get("domain_files_custom_3")).toDrsObjectId(),
        equalTo(String.format("v1_%s_%s", snapshotId, file4Model.getFileId())));

    // Assert that 2 drs ids were loaded
    assertThat("2 drs ids are present", drsIds, hasSize(2));
    // Ensure that all DRS can be parsed
    List<String> drsObjectIds =
        drsIds.stream()
            .map(DrsIdService::fromUri)
            .map(DrsId::toDrsObjectId)
            .collect(Collectors.toList());

    String fileId = result.getLoadFileResults().get(0).getFileId();
    String filePath = result.getLoadFileResults().get(0).getTargetPath();

    // Do a Drs lookup
    String drsId = String.format("v1_%s_%s", snapshotId, fileId);
    assertThat("Expected Drs object Id exists", drsObjectIds.contains(drsId));
    DRSObject drsObject = dataRepoFixtures.drsGetObject(steward(), drsId);
    assertThat("DRS object has single access method", drsObject.getAccessMethods(), hasSize(1));
    assertThat(
        "DRS object has HTTPS",
        drsObject.getAccessMethods().get(0).getType(),
        equalTo(TypeEnum.HTTPS));
    assertThat(
        "DRS object has access id",
        drsObject.getAccessMethods().get(0).getAccessId(),
        equalTo("az-centralus"));
    // Make sure we can read the drs object
    DrsResponse<DRSAccessURL> access =
        dataRepoFixtures.getObjectAccessUrl(steward(), drsId, "az-centralus");
    assertThat("Returns DRS access", access.getResponseObject().isPresent(), is(true));
    String signedUrl = access.getResponseObject().get().getUrl();

    TestUtils.verifyHttpAccess(signedUrl, Map.of());
    verifySignedUrl(signedUrl, steward(), "r");

    // Create snapshot by row id
    SnapshotSummaryModel snapshotSummaryByRowId =
        dataRepoFixtures.createSnapshotWithRequest(
            steward(), summaryModel.getName(), profileId, snapshotByRowIdModel);
    snapshotByRowId = snapshotSummaryByRowId.getId();
    assertThat(
        "Snapshot exists",
        snapshotSummaryByRowId.getName(),
        equalTo(snapshotByRowIdModel.getName()));

    // Read the ingested metadata
    AccessInfoParquetModel snapshotByRowIdParquetAccessInfo =
        dataRepoFixtures
            .getSnapshot(
                steward(),
                snapshotByRowId,
                List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION))
            .getAccessInformation()
            .getParquet();

    String snapshotByRowIdParquetUrl =
        snapshotByRowIdParquetAccessInfo.getUrl()
            + "?"
            + snapshotByRowIdParquetAccessInfo.getSasToken();
    TestUtils.verifyHttpAccess(snapshotByRowIdParquetUrl, Map.of());
    verifySignedUrl(snapshotByRowIdParquetUrl, steward(), "rl");

    for (AccessInfoParquetModelTable table : snapshotByRowIdParquetAccessInfo.getTables()) {
      if (tablesToCheck.contains(table.getName())) {
        String tableUrl = table.getUrl() + "?" + table.getSasToken();
        TestUtils.verifyHttpAccess(tableUrl, Map.of());
        verifySignedUrl(tableUrl, steward(), "rl");
      }
    }

    // Do a Drs lookup
    String drsIdByRowId = String.format("v1_%s_%s", snapshotByRowId, fileId);
    DRSObject drsObjectByRowId = dataRepoFixtures.drsGetObject(steward(), drsIdByRowId);
    assertThat(
        "DRS object has single access method",
        drsObjectByRowId.getAccessMethods().size(),
        equalTo(1));
    assertThat(
        "DRS object has HTTPS",
        drsObjectByRowId.getAccessMethods().get(0).getType(),
        equalTo(TypeEnum.HTTPS));
    assertThat(
        "DRS object has access id",
        drsObjectByRowId.getAccessMethods().get(0).getAccessId(),
        equalTo("az-centralus"));
    // Make sure we can read the drs object
    DrsResponse<DRSAccessURL> accessForByRowId =
        dataRepoFixtures.getObjectAccessUrl(steward(), drsIdByRowId, "az-centralus");
    assertThat("Returns DRS access", accessForByRowId.getResponseObject().isPresent(), is(true));
    String signedUrlForByRowId = accessForByRowId.getResponseObject().get().getUrl();

    TestUtils.verifyHttpAccess(signedUrlForByRowId, Map.of());
    verifySignedUrl(signedUrlForByRowId, steward(), "r");

    // Delete dataset should fail
    dataRepoFixtures.deleteDatasetShouldFail(steward, datasetId);

    // Delete snapshot
    dataRepoFixtures.deleteSnapshot(steward, snapshotId);
    dataRepoFixtures.deleteSnapshot(steward, snapshotByRowId);

    dataRepoFixtures.assertFailToGetSnapshot(steward(), snapshotId);
    dataRepoFixtures.assertFailToGetSnapshot(steward(), snapshotByRowId);
    snapshotId = null;

    // Delete the file we just ingested
    dataRepoFixtures.deleteFile(steward, datasetId, fileId);

    assertThat(
        "file is gone",
        dataRepoFixtures.getFileByIdRaw(steward, datasetId, fileId).getStatusCode(),
        equalTo(HttpStatus.NOT_FOUND));

    assertThat(
        "file is gone",
        dataRepoFixtures.getFileByNameRaw(steward, datasetId, filePath).getStatusCode(),
        equalTo(HttpStatus.NOT_FOUND));

    // Delete dataset should now succeed
    dataRepoFixtures.deleteDataset(steward, datasetId);
    datasetId = null;

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
            .sourcePath(blobIOTestUtility.createSourcePath(sourceFile))
            .targetPath("/test/target.txt");
    BulkLoadFileModel fileLoadModelSas =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                blobIOTestUtility.createSourceSignedPath(
                    sourceFile, getSourceStorageAccountPrimarySharedKey()))
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
            .sourcePath(blobIOTestUtility.createSourcePath(sourceFile))
            .targetPath("/test/target2.txt");
    BulkLoadFileModel fileLoadModelSas2 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                blobIOTestUtility.createSourceSignedPath(
                    sourceFile, getSourceStorageAccountPrimarySharedKey()))
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
            .sourcePath(blobIOTestUtility.createSourcePath(sourceFile))
            .targetPath("/test/target3.txt");
    BulkLoadFileModel fileLoadModelSas3 =
        new BulkLoadFileModel()
            .mimeType("text/plain")
            .sourcePath(
                blobIOTestUtility.createSourceSignedPath(
                    sourceFile, getSourceStorageAccountPrimarySharedKey()))
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

  @Test
  public void testDatasetFileRefValidation() throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "dataset-ingest-azure-fileref.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();

    String noFilesContents =
        "sample_name,data_type,vcf_file_ref,vcf_index_file_ref\n"
            + String.format("NA12878_none,none,%s,%s", UUID.randomUUID(), UUID.randomUUID());
    String noFilesControlFile =
        blobIOTestUtility.uploadFileWithContents("dataset-files-ingest-fail.csv", noFilesContents);

    IngestRequestModel noFilesIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(noFilesControlFile)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test2"))
            .csvSkipLeadingRows(2);

    DataRepoResponse<IngestResponseModel> noFilesIngestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward, datasetId, noFilesIngestRequest);

    assertThat(
        "No files yet loaded doesn't result in an NPE",
        noFilesIngestResponse.getErrorObject().get().getMessage(),
        equalTo("Invalid file ids found during ingest (2 returned in details)"));

    String loadTag = UUID.randomUUID().toString();
    var arrayRequestModel =
        new BulkLoadArrayRequestModel()
            .profileId(summaryModel.getDefaultProfileId())
            .loadTag(loadTag);

    long fileSize = MIB / 10;
    Stream.of(
            "NA12878_PLUMBING_exome.g.vcf.gz",
            "NA12878_PLUMBING_exome.g.vcf.gz.tbi",
            "NA12878_PLUMBING_wgs.g.vcf.gz",
            "NA12878_PLUMBING_wgs.g.vcf.gz.tbi")
        .map(
            name -> {
              String sourceFile = blobIOTestUtility.uploadSourceFile(name, fileSize);
              return new BulkLoadFileModel()
                  .sourcePath(blobIOTestUtility.createSourcePath(sourceFile))
                  .targetPath("/vcfs/downsampled/" + name)
                  .description("Test file for " + name)
                  .mimeType("text/plain");
            })
        .forEach(arrayRequestModel::addLoadArrayItem);

    var bulkLoadArrayResultModel =
        dataRepoFixtures.bulkLoadArray(steward, datasetId, arrayRequestModel);

    var resultModels =
        bulkLoadArrayResultModel.getLoadFileResults().stream()
            .collect(
                Collectors.toMap(
                    m -> m.getTargetPath().replaceAll("/vcfs/downsampled/NA12878_PLUMBING_", ""),
                    Function.identity()));
    var exomeVcf = resultModels.get("exome.g.vcf.gz");
    var exomeVcfIndex = resultModels.get("exome.g.vcf.gz.tbi");
    var wgsVcf = resultModels.get("wgs.g.vcf.gz");
    var wgsVcfIndex = resultModels.get("wgs.g.vcf.gz.tbi");

    var datasetMetadata =
        Files.readString(
            ResourceUtils.getFile("classpath:dataset-ingest-combined-metadata-only.csv").toPath());
    var metadataWithFileIds =
        datasetMetadata
            .replaceFirst("EXOME_VCF_FILE_REF", exomeVcf.getFileId())
            .replaceFirst("EXOME_VCF_INDEX_FILE_REF", exomeVcfIndex.getFileId())
            .replaceFirst("WGS_VCF_FILE_REF", wgsVcf.getFileId())
            .replaceFirst("WGS_VCF_INDEX_FILE_REF", wgsVcfIndex.getFileId());

    String controlFile =
        blobIOTestUtility.uploadFileWithContents("dataset-files-ingest.csv", metadataWithFileIds);
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(controlFile)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test"))
            .csvSkipLeadingRows(2);

    IngestResponseModel ingestResponseJson =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequest);

    assertThat(
        "there are two successful ingest rows", ingestResponseJson.getRowCount(), equalTo(2L));
    assertThat("there were no bad ingest rows", ingestResponseJson.getBadRowCount(), equalTo(0L));

    IngestRequestModel failingIngestRequest =
        new IngestRequestModel()
            .format(IngestRequestModel.FormatEnum.CSV)
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .path(noFilesControlFile)
            .profileId(profileId)
            .loadTag(Names.randomizeName("test2"))
            .csvSkipLeadingRows(2);

    DataRepoResponse<IngestResponseModel> failingIngestResponse =
        dataRepoFixtures.ingestJsonDataRaw(steward, datasetId, failingIngestRequest);

    assertThat(
        "Failing fileIds return an error",
        failingIngestResponse.getErrorObject().isPresent(),
        is(true));

    assertThat(
        "2 invalid ids were returned",
        failingIngestResponse.getErrorObject().get().getErrorDetail(),
        hasSize(2));

    // Make sure that any failure in tearing down is presented as a test failure
    blobIOTestUtility.deleteContainers();
    clearEnvironment();
  }

  @Test
  public void testDatasetCombinedIngest() throws Exception {
    testDatasetCombinedIngest(true);
  }

  @Test
  public void testDatasetCombinedIngestFromApi() throws Exception {
    testDatasetCombinedIngest(false);
  }

  public void testDatasetCombinedIngest(boolean ingestFromFile) throws Exception {
    DatasetSummaryModel summaryModel =
        dataRepoFixtures.createDataset(
            steward, profileId, "dataset-ingest-combined-azure.json", CloudPlatform.AZURE);
    datasetId = summaryModel.getId();

    String controlFileContents;
    try (var resourceStream =
        this.getClass().getResourceAsStream("/dataset-ingest-combined-control-azure.json")) {
      controlFileContents = new String(resourceStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .ignoreUnknownValues(false)
            .maxBadRecords(0)
            .table("sample_vcf")
            .profileId(profileId)
            .loadTag(Names.randomizeName("azureCombinedIngestTest"));

    if (ingestFromFile) {
      String controlFile =
          blobIOTestUtility.uploadFileWithContents(
              "dataset-files-ingest-combined.json", controlFileContents);
      ingestRequest.path(controlFile).format(IngestRequestModel.FormatEnum.JSON);
    } else {
      List<Map<String, Object>> data =
          Arrays.stream(controlFileContents.split("\\n"))
              .map(j -> jsonLoader.loadJson(j, new TypeReference<Map<String, Object>>() {}))
              .collect(Collectors.toList());
      ingestRequest
          .records(Arrays.asList(data.toArray()))
          .format(IngestRequestModel.FormatEnum.ARRAY);
    }

    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward, datasetId, ingestRequest);

    dataRepoFixtures.assertCombinedIngestCorrect(ingestResponse, steward);

    clearEnvironment();
  }

  private void clearEnvironment() throws Exception {
    if (snapshotId != null) {
      dataRepoFixtures.deleteSnapshot(steward, snapshotId);
      snapshotId = null;
    }

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

  private void verifySignedUrl(String signedUrl, User user, String expectedPermissions) {
    BlobUrlParts blobUrlParts = BlobUrlParts.parse(signedUrl);
    assertThat(
        "Signed url contains user",
        blobUrlParts.getCommonSasQueryParameters().getContentDisposition(),
        equalTo(user.getEmail()));
    assertThat(
        "Signed url only contains expected permissions",
        blobUrlParts.getCommonSasQueryParameters().getPermissions(),
        equalTo(expectedPermissions));
  }
}
