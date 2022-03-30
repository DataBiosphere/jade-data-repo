package bio.terra.service.dataset;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.logging.v2.LifecycleState;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class DatasetConnectedTest {

  @Autowired private MockMvc mvc;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ResourceService dataLocationService;
  @Autowired private DatasetDao datasetDao;
  @Autowired private ConfigurationService configService;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ApplicationConfiguration applicationConfiguration;
  @MockBean private IamProviderInterface samService;
  @Autowired private GoogleResourceManagerService googleResourceManagerService;
  @Autowired private DatasetBucketDao datasetBucketDao;
  @Autowired private GoogleResourceDao googleResourceDao;

  private DatasetRequestModel datasetRequest;
  private DatasetSummaryModel summaryModel;
  private static final Logger logger = LoggerFactory.getLogger(DatasetConnectedTest.class);

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    BillingProfileModel billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    // create a dataset and check that it succeeds
    String resourcePath = "snapshot-test-dataset.json";
    datasetRequest = jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
    datasetRequest
        .name(Names.randomizeName(datasetRequest.getName()))
        .defaultProfileId(billingProfile.getId());
    summaryModel = connectedOperations.createDataset(datasetRequest);
    logger.info("--------begin test---------");
  }

  @After
  public void tearDown() throws Exception {
    logger.info("--------start of tear down---------");

    configService.reset();
    connectedOperations.teardown();
  }

  @Test
  public void testDuplicateName() throws Exception {
    assertNotNull("created dataset successfully the first time", summaryModel);

    // fetch the dataset and confirm the metadata matches the request
    DatasetModel datasetModel = connectedOperations.getDataset(summaryModel.getId());
    assertNotNull("fetched dataset successfully after creation", datasetModel);
    assertEquals(
        "fetched dataset name matches request", datasetRequest.getName(), datasetModel.getName());

    // check that the dataset metadata row is unlocked
    String exclusiveLock = datasetDao.getExclusiveLock(summaryModel.getId());
    assertNull("dataset row is unlocked", exclusiveLock);

    // try to create the same dataset again and check that it fails
    datasetRequest.description("Make sure nothing is getting overwritten");
    ErrorModel errorModel =
        connectedOperations.createDatasetExpectError(datasetRequest, HttpStatus.BAD_REQUEST);
    assertThat(
        "error message includes name conflict",
        errorModel.getMessage(),
        containsString("Dataset name or id already exists"));

    // fetch the dataset and confirm the metadata still matches the original
    DatasetModel origModel = connectedOperations.getDataset(summaryModel.getId());
    assertEquals("fetched dataset remains unchanged", datasetModel, origModel);

    // delete the dataset and check that it succeeds
    connectedOperations.deleteTestDatasetAndCleanup(summaryModel.getId());

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testOverlappingDeletes() throws Exception {
    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in DeleteDatasetPrimaryDataStep
    configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // Make sure that dataset delete fails on lock conflict
    configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_SKIP_RETRY_FAULT.name(), true);

    // try to delete the dataset
    MvcResult result1 =
        mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // try to delete the dataset again
    MvcResult result2 =
        mvc.perform(delete("/api/repository/v1/datasets/" + summaryModel.getId())).andReturn();
    TimeUnit.SECONDS.sleep(5); // give the flight time to launch

    // disable hang in DeleteDatasetPrimaryDataStep
    configService.setFault(ConfigEnum.DATASET_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
    // ====================================================

    // check the response from the first delete request
    MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
    DeleteResponseModel deleteResponseModel =
        connectedOperations.handleSuccessCase(response1, DeleteResponseModel.class);
    assertEquals(
        "First delete returned successfully",
        DeleteResponseModel.ObjectStateEnum.DELETED,
        deleteResponseModel.getObjectState());

    // check that the second delete failed with a lock exception
    MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
    ErrorModel errorModel2 =
        connectedOperations.handleFailureCase(response2, HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(
        "delete failed on lock exception",
        errorModel2.getMessage(),
        startsWith("Failed to lock the dataset"));

    // try to fetch the dataset again and confirm nothing is returned
    connectedOperations.getDatasetExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testCSVIngestAddsRowIdsByDefault() throws Exception {
    String resourceFileName = "snapshot-test-dataset-data.csv";
    String dirInCloud = "scratch/testAddRowIds/" + UUID.randomUUID();
    String tableIngestInputFilePath = uploadIngestInputFile(resourceFileName, dirInCloud);
    // ingest the table
    String tableName = "thetable";
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .path(tableIngestInputFilePath)
            .csvGenerateRowIds(true);
    connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

    String columns = PdaoConstant.PDAO_ROW_ID_COLUMN + ",thecolumn";
    TableResult bqQueryResult =
        TestUtils.selectFromBigQueryDataset(
            datasetDao, dataLocationService, datasetRequest.getName(), tableName, columns);
    List<UUID> rowIds = new ArrayList<>();
    Set<String> expectedNames = Set.of("Andrea", "Dan", "Rori", "Jeremy");
    Set<String> datasetNames = new HashSet<>();
    bqQueryResult
        .iterateAll()
        .forEach(
            r -> {
              rowIds.add(UUID.fromString(r.get(PdaoConstant.PDAO_ROW_ID_COLUMN).getStringValue()));
              datasetNames.add(r.get("thecolumn").getStringValue());
            });
    assertEquals(rowIds.size(), 4);
    assertEquals(expectedNames, datasetNames);
  }

  @Test
  public void testCSVIngestUpdates() throws Exception {
    String resourceFileName = "snapshot-test-dataset-data.csv";
    String dirInCloud = "scratch/testAddRowIds/" + UUID.randomUUID();
    String tableIngestInputFilePath = uploadIngestInputFile(resourceFileName, dirInCloud);
    // ingest the table
    String tableName = "thetable";
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .path(tableIngestInputFilePath)
            .csvGenerateRowIds(true);
    connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

    String columns = PdaoConstant.PDAO_ROW_ID_COLUMN + ",thecolumn";
    TableResult bqQueryResult =
        TestUtils.selectFromBigQueryDataset(
            datasetDao, dataLocationService, datasetRequest.getName(), tableName, columns);
    List<UUID> rowIds = new ArrayList<>();
    Set<String> expectedNames = Set.of("Andrea", "Dan", "Rori", "Jeremy");
    Set<String> datasetNames = new HashSet<>();
    bqQueryResult
        .iterateAll()
        .forEach(
            r -> {
              rowIds.add(UUID.fromString(r.get(PdaoConstant.PDAO_ROW_ID_COLUMN).getStringValue()));
              datasetNames.add(r.get("thecolumn").getStringValue());
            });
    assertEquals(rowIds.size(), 4);
    assertEquals(expectedNames, datasetNames);
  }

  @Test
  public void validateBulkIngestControlFile() throws Exception {
    String resourceFileName = "dataset-ingest-control-file-invalid.json";
    String dirInCloud = "scratch/validateBulkIngestControlFile/" + UUID.randomUUID();
    String ingestControlFilePath = uploadIngestInputFile(resourceFileName, dirInCloud);

    String bulkLoadTag = Names.randomizeName("loadTag");
    BulkLoadRequestModel request =
        new BulkLoadRequestModel()
            .loadControlFile(ingestControlFilePath)
            .loadTag(bulkLoadTag)
            .profileId(summaryModel.getDefaultProfileId());
    ErrorModel errorModel =
        connectedOperations.ingestBulkFileFailure(summaryModel.getId(), request);
    assertThat(
        "Error message detail should include that the sourcePath and targetPath were not defined in the control file.",
        errorModel.getErrorDetail().get(0),
        containsString("The following required field(s) were not defined: sourcePath, targetPath"));
  }

  @Test
  public void testMetadataTableUpdate() throws Exception {
    String resourceFileName = "snapshot-test-dataset-data.csv";
    String dirInCloud = "scratch/testAddRowIds/" + UUID.randomUUID();
    String tableIngestInputFilePath = uploadIngestInputFile(resourceFileName, dirInCloud);
    // ingest the table
    String tableName = "thetable";
    String loadTag = UUID.randomUUID().toString();
    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table(tableName)
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(1)
            .path(tableIngestInputFilePath)
            .csvGenerateRowIds(true)
            .loadTag(loadTag);
    connectedOperations.ingestTableSuccess(summaryModel.getId(), ingestRequest);

    Optional<DatasetTable> table =
        datasetDao.retrieve(summaryModel.getId()).getTableByName(tableName);
    String metadataTableName;
    if (table.isPresent()) {
      metadataTableName = table.get().getRowMetadataTableName();
    } else {
      throw new TableNotFoundException("Table not found: " + tableName);
    }
    List<String> columns =
        List.of(
            PdaoConstant.PDAO_ROW_ID_COLUMN,
            PdaoConstant.PDAO_INGESTED_BY_COLUMN,
            PdaoConstant.PDAO_INGEST_TIME_COLUMN,
            PdaoConstant.PDAO_LOAD_TAG_COLUMN);
    TableResult bqQueryResult =
        TestUtils.selectFromBigQueryDataset(
            datasetDao,
            dataLocationService,
            datasetRequest.getName(),
            metadataTableName,
            String.join(",", columns));
    Set<UUID> rowIds = new HashSet<>();
    Set<String> ingestedBy = new HashSet<>();
    Set<Long> ingestTime = new HashSet<>();
    Set<String> loadTags = new HashSet<>();
    bqQueryResult
        .iterateAll()
        .forEach(
            r -> {
              rowIds.add(UUID.fromString(r.get(PdaoConstant.PDAO_ROW_ID_COLUMN).getStringValue()));
              ingestedBy.add(r.get(PdaoConstant.PDAO_INGESTED_BY_COLUMN).getStringValue());
              ingestTime.add(r.get(PdaoConstant.PDAO_INGEST_TIME_COLUMN).getTimestampValue());
              loadTags.add(r.get(PdaoConstant.PDAO_LOAD_TAG_COLUMN).getStringValue());
            });
    assertEquals(rowIds.size(), 4);
    assertEquals(ingestedBy.size(), 1);
    assertEquals(ingestedBy.iterator().next(), applicationConfiguration.getUserEmail());
    assertEquals(ingestTime.size(), 1);
    assertEquals(loadTags.size(), 1);
    assertEquals(loadTags.iterator().next(), loadTag);
  }

  @Test
  public void testProjectDeleteAfterDatasetDelete() throws Exception {
    String resourcePath = "snapshot-test-dataset.json";
    // --- create billing profile and dataset ----
    BillingProfileModel billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
    datasetRequest
        .name(Names.randomizeName(datasetRequest.getName()))
        .defaultProfileId(billingProfile.getId());
    DatasetSummaryModel summaryModel = connectedOperations.createDataset(datasetRequest);
    // retrieve dataset and store project id
    DatasetModel datasetModel = connectedOperations.getDataset(summaryModel.getId());
    assertNotNull("fetched dataset successfully after creation", datasetModel);
    String datasetGoogleProjectId = datasetModel.getDataProject();
    assertNotNull(
        "Dataset google project should now exist",
        googleResourceManagerService.getProject(datasetGoogleProjectId));

    // --- Ingest into dataset with a different billing profile ---
    // create a different billing profile
    BillingProfileModel billingProfile_diff =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    // ingest a file
    URI sourceUri = new URI("gs", "jade-testdata", "/fileloadprofiletest/1KBfile.txt", null, null);
    String targetFilePath = "/tdr/" + Names.randomizeName("testdir") + "/testProjectDelete.txt";
    FileLoadModel fileLoadModel =
        new FileLoadModel()
            .sourcePath(sourceUri.toString())
            .description("testExcludeLockedFromSnapshotFileLookups")
            .mimeType("text/plain")
            .targetPath(targetFilePath)
            .profileId(billingProfile_diff.getId());
    FileModel fileModel =
        connectedOperations.ingestFileSuccess(summaryModel.getId(), fileLoadModel);

    // Retrieve list of projects associated with dataset/bucket
    // only one bucket b/c we didn't ingest anything with the first billing profile
    List<UUID> projectResourceIds =
        datasetBucketDao.getProjectResourceIdsForBucketPerDataset(summaryModel.getId());
    String ingestGoogleProjectId =
        googleResourceDao.retrieveProjectById(projectResourceIds.get(0)).getGoogleProjectId();
    assertThat(
        "The dataset google project is different from ingest bucket google project that used a different billing profile",
        ingestGoogleProjectId,
        not(datasetGoogleProjectId));
    assertNotNull(
        "Ingest google project should now exist",
        googleResourceManagerService.getProject(ingestGoogleProjectId));

    // --- delete dataset and confirm both google projects were also deleted ---
    connectedOperations.deleteTestDataset(datasetModel.getId());
    connectedOperations.getDatasetExpectError(datasetModel.getId(), HttpStatus.NOT_FOUND);

    assertThat(
        "Dataset google project should be marked for delete",
        googleResourceManagerService.getProject(datasetGoogleProjectId).getLifecycleState(),
        equalTo(LifecycleState.DELETE_REQUESTED.toString()));
    assertThat(
        "Ingest google project should be marked for delete",
        googleResourceManagerService.getProject(ingestGoogleProjectId).getLifecycleState(),
        equalTo(LifecycleState.DELETE_REQUESTED.toString()));
    // We don't need to clean up the file in connected operations cleanup since the project was
    // deleted
    connectedOperations.removeFile(summaryModel.getId(), fileModel.getFileId());
  }

  private String uploadIngestInputFile(String resourceFileName, String dirInCloud)
      throws IOException {
    BlobInfo ingestTableBlob =
        BlobInfo.newBuilder(testConfig.getIngestbucket(), dirInCloud + "/" + resourceFileName)
            .build();
    Storage storage = StorageOptions.getDefaultInstance().getService();
    storage.create(
        ingestTableBlob,
        IOUtils.toByteArray(getClass().getClassLoader().getResource(resourceFileName)));
    return "gs://" + testConfig.getIngestbucket() + "/" + dirInCloud + "/" + resourceFileName;
  }
}
