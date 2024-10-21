package bio.terra.service.dataset;

import static bio.terra.common.TestUtils.assertError;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.AssetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.IngestRequestModel.UpdateStrategyEnum;
import bio.terra.model.JobModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidAssetException;
import bio.terra.service.dataset.flight.ingest.DatasetIngestFlight;
import bio.terra.service.dataset.flight.ingest.scratch.DatasetScratchFilePrepareFlight;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import com.azure.resourcemanager.loganalytics.models.Workspace;
import com.azure.resourcemanager.monitor.models.DiagnosticSetting;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class DatasetServiceTest {
  private AuthenticatedUserRequest testUser =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DatasetDao datasetDao;

  @Autowired private DatasetService datasetService;

  @SpyBean private JobService jobService;

  @MockBean private IamProviderInterface samService;

  @Autowired private ConnectedOperations connectedOperations;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  @Autowired private NamedParameterJdbcTemplate jdbcTemplate;

  @MockBean private ResourceService resourceService;
  @MockBean private GcsPdao gcsPdao;
  @MockBean private AzureContainerPdao azureContainerPdao;
  @MockBean private AzureBlobStorePdao azureBlobStorePdao;
  @MockBean private AzureMonitoringService azureMonitoringService;
  @MockBean private MetadataDataAccessUtils metadataDataAccessUtils;
  @MockBean private AzureSynapsePdao azureSynapsePdao;

  @Captor private ArgumentCaptor<List<String>> listCaptor;
  @Captor private ArgumentCaptor<IngestRequestModel> requestCaptor;

  private BillingProfileModel billingProfile;
  private UUID projectId;
  private ArrayList<String> flightIdsList;
  private ArrayList<UUID> datasetIdList;

  private UUID createDataset(DatasetRequestModel datasetRequest, String newName)
      throws IOException, SQLException {
    datasetRequest.name(newName).defaultProfileId(billingProfile.getId());
    Dataset dataset =
        DatasetUtils.convertRequestWithGeneratedNames(datasetRequest)
            .projectResourceId(projectId)
            .projectResource(resourceDao.retrieveProjectById(projectId));
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(datasetId, createFlightId);
    datasetIdList.add(datasetId);
    return datasetId;
  }

  private UUID createDataset(String datasetFile) throws IOException, SQLException {
    return createDataset(datasetFile, CloudPlatform.AZURE);
  }

  private UUID createDataset(String datasetFile, CloudPlatform platform)
      throws IOException, SQLException {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
    datasetRequest.setCloudPlatform(platform);
    UUID datasetId = createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID());
    datasetIdList.add(datasetId);
    return datasetId;
  }

  @Before
  public void setup() throws Exception {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "hi@hi.hi");
    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    projectId = resourceDao.createProject(projectResource);
    when(resourceService.getProjectResource(projectId)).thenReturn(projectResource);

    // Setup mock sam service
    connectedOperations.stubOutSamCalls(samService);
    flightIdsList = new ArrayList<>();
    datasetIdList = new ArrayList<>();
  }

  @After
  public void teardown() {
    for (UUID datasetId : datasetIdList) {
      datasetDao.delete(datasetId);
    }
    resourceDao.deleteProject(projectId);
    profileDao.deleteBillingProfileById(billingProfile.getId());
  }

  @Test
  public void datasetOmopTest() throws IOException, SQLException {
    createDataset("omop/it-dataset-omop.jsonl");
  }

  @Test(expected = DatasetNotFoundException.class)
  public void datasetDeleteTest() throws IOException, SQLException {
    UUID datasetId = createDataset("dataset-create-test.json");
    assertThat("dataset delete signals success", datasetDao.delete(datasetId), equalTo(true));
    datasetDao.retrieve(datasetId);
  }

  @Test
  public void addDatasetAssetSpecifications() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");
    String assetName = "assetName";
    // get created dataset
    Dataset createdDataset = datasetDao.retrieve(datasetId);
    assertThat(
        "dataset already has two asset specs",
        createdDataset.getAssetSpecifications().size(),
        equalTo(2));

    AssetModel assetModel =
        new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    // add asset to dataset
    String jobId =
        datasetService.addDatasetAssetSpecifications(datasetId.toString(), assetModel, testUser);
    flightIdsList.add(jobId);

    TestUtils.eventualExpect(
        5,
        60,
        true,
        () ->
            jobService
                .retrieveJob(jobId, testUser)
                .getJobStatus()
                .equals(JobModel.JobStatusEnum.SUCCEEDED));

    // get dataset
    Dataset dataset = datasetDao.retrieve(datasetId);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has an additional asset spec",
        dataset.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    datasetDao.delete(datasetId);
  }

  @Test
  public void addMultipleDatasetAssetSpecificationsShouldFail() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");
    String assetName = "assetName";
    // get created dataset
    Dataset createdDataset = datasetDao.retrieve(datasetId);
    assertThat(
        "dataset already has two asset specs",
        createdDataset.getAssetSpecifications().size(),
        equalTo(2));

    AssetModel assetModel1 =
        new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    AssetModel assetModel2 =
        new AssetModel()
            .name(assetName)
            .rootTable("participant")
            .rootColumn("id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    // add first asset to the dataset
    String jobId1 =
        datasetService.addDatasetAssetSpecifications(datasetId.toString(), assetModel1, testUser);
    flightIdsList.add(jobId1);

    boolean assetAdd1 =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () ->
                jobService
                    .retrieveJob(jobId1, testUser)
                    .getJobStatus()
                    .equals(JobModel.JobStatusEnum.SUCCEEDED));
    Assert.assertTrue(assetAdd1);

    // get dataset
    Dataset dataset = datasetDao.retrieve(datasetId);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has an additional asset spec",
        dataset.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    // add second asset to dataset, this should fail because it has the same name as the first
    String jobId2 =
        datasetService.addDatasetAssetSpecifications(datasetId.toString(), assetModel2, testUser);
    flightIdsList.add(jobId2);

    boolean assetAdd2 =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () ->
                jobService
                    .retrieveJob(jobId2, testUser)
                    .getJobStatus()
                    .equals(JobModel.JobStatusEnum.FAILED));
    Assert.assertTrue(assetAdd2);

    // make sure the first asset we created hasn't been deleted during the undo step
    assertThat(
        "dataset has an additional asset spec",
        dataset.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    datasetDao.delete(datasetId);
  }

  @Test
  public void addAssetSpecWithSameNameToMultipleDatasetsShouldPass() throws Exception {
    UUID datasetId1 = createDataset("dataset-create-test.json");
    UUID datasetId2 = createDataset("dataset-create-test.json");
    String assetName = "assetName";
    // get created dataset
    Dataset createdDataset = datasetDao.retrieve(datasetId1);
    assertThat(
        "dataset already has two asset specs",
        createdDataset.getAssetSpecifications().size(),
        equalTo(2));

    AssetModel assetModel =
        new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    // add first asset to the dataset
    String jobId1 =
        datasetService.addDatasetAssetSpecifications(datasetId1.toString(), assetModel, testUser);
    flightIdsList.add(jobId1);

    boolean assetAdd1 =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () ->
                jobService
                    .retrieveJob(jobId1, testUser)
                    .getJobStatus()
                    .equals(JobModel.JobStatusEnum.SUCCEEDED));
    Assert.assertTrue(assetAdd1);

    // get dataset 1
    Dataset dataset = datasetDao.retrieve(datasetId1);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has an additional asset spec",
        dataset.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    // add asset tp second dataset
    String jobId2 =
        datasetService.addDatasetAssetSpecifications(datasetId2.toString(), assetModel, testUser);
    flightIdsList.add(jobId2);

    boolean assetAdd2 =
        TestUtils.eventualExpect(
            5,
            60,
            true,
            () ->
                jobService
                    .retrieveJob(jobId2, testUser)
                    .getJobStatus()
                    .equals(JobModel.JobStatusEnum.SUCCEEDED));
    Assert.assertTrue(assetAdd2);

    Dataset dataset2 = datasetDao.retrieve(datasetId2);

    // make sure the second dataset has the expected asset
    assertThat(
        "dataset has an additional asset spec",
        dataset2.getAssetSpecifications().size(),
        equalTo(3));
    assertThat(
        "dataset has expected asset",
        dataset2.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    datasetDao.delete(datasetId1);
    datasetDao.delete(datasetId2);
  }

  @Test
  public void addDatasetBadAssetSpecification() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");
    String assetName = "sample"; // This asset name already exists
    // get created dataset
    Dataset createdDataset = datasetDao.retrieve(datasetId);
    assertThat(
        "dataset already has two asset specs",
        createdDataset.getAssetSpecifications().size(),
        equalTo(2));

    AssetModel assetModel =
        new AssetModel()
            .name(assetName)
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(
                Arrays.asList(
                    DatasetFixtures.buildAssetParticipantTable(),
                    DatasetFixtures.buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));

    // add asset to dataset
    String jobId =
        datasetService.addDatasetAssetSpecifications(datasetId.toString(), assetModel, testUser);
    flightIdsList.add(jobId);

    TestUtils.eventualExpect(
        5,
        60,
        true,
        () ->
            jobService
                .retrieveJob(jobId, testUser)
                .getJobStatus()
                .equals(JobModel.JobStatusEnum.FAILED));

    try {
      try {
        jobService.retrieveJobResult(jobId, ErrorModel.class, testUser);
        fail("Expected invalid asset exception");
      } catch (InvalidAssetException ex) {
        assertThat(
            "error message is correct",
            ex.getMessage(),
            equalTo("Asset name already exists: sample"));
        // get dataset
        Dataset dataset = datasetDao.retrieve(datasetId);

        // make sure the dataset has the expected asset
        assertThat(
            "dataset has no additional asset spec",
            dataset.getAssetSpecifications().size(),
            equalTo(2));
      }
    } finally {
      datasetDao.delete(datasetId);
    }
  }

  @Test
  public void removeDatasetAssetSpecifications() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json");
    String assetName = "sample";

    // get dataset
    Dataset datasetWAssets = datasetDao.retrieve(datasetId);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has two asset specs already",
        datasetWAssets.getAssetSpecifications().size(),
        equalTo(2));
    assertThat(
        "dataset has expected assets",
        datasetWAssets.getAssetSpecificationByName(assetName).isPresent(),
        equalTo(true));

    // remove asset from dataset
    String jobId =
        datasetService.removeDatasetAssetSpecifications(datasetId.toString(), assetName, testUser);
    flightIdsList.add(jobId);

    TestUtils.eventualExpect(
        5,
        60,
        true,
        () ->
            jobService
                .retrieveJob(jobId, testUser)
                .getJobStatus()
                .equals(JobModel.JobStatusEnum.SUCCEEDED));

    // get dataset
    Dataset dataset = datasetDao.retrieve(datasetId);

    // make sure the dataset has the expected asset
    assertThat(
        "dataset has one less asset spec", dataset.getAssetSpecifications().size(), equalTo(1));

    datasetDao.delete(datasetId);
  }

  @Test
  public void retrieveDatasetDefault() throws SQLException, IOException {
    UUID datasetId = createDataset("dataset-create-test.json");
    Dataset dataset = datasetDao.retrieve(datasetId);
    assertThat(
        "dataset info defaults are expected",
        datasetService.retrieveModel(dataset, testUser),
        equalTo(
            datasetService.retrieveModel(
                dataset,
                testUser,
                List.of(
                    DatasetRequestAccessIncludeModel.SCHEMA,
                    DatasetRequestAccessIncludeModel.PROFILE,
                    DatasetRequestAccessIncludeModel.DATA_PROJECT,
                    DatasetRequestAccessIncludeModel.STORAGE))));
  }

  @Test
  public void ingestPayloadDataGcp() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json", CloudPlatform.GCP);
    String bucketName = "mybucket";
    GoogleBucketResource bucket = mock(GoogleBucketResource.class);
    when(bucket.getName()).thenReturn(bucketName);
    when(resourceService.getOrCreateBucketForBigQueryScratchFile(any(), any())).thenReturn(bucket);

    IngestRequestModel ingestRequestModel =
        new IngestRequestModel()
            .loadTag("lt")
            .format(FormatEnum.ARRAY)
            .updateStrategy(UpdateStrategyEnum.APPEND)
            .table("participant")
            .addRecordsItem(Map.of("id", "1", "age", 12, "gender", "F"))
            .addRecordsItem(Map.of("id", "2", "age", 24, "gender", "N"))
            .addRecordsItem(Map.of("id", "3", "age", 36, "gender", "M"));

    datasetService.ingestDataset(datasetId.toString(), ingestRequestModel, testUser);

    verify(gcsPdao, times(1)).writeListToCloudFile(any(), listCaptor.capture(), any());

    JSONAssert.assertEquals(
        "correct lines were written",
        String.join("\n", listCaptor.getValue()),
        "{\"id\":\"1\",\"age\":12,\"gender\":\"F\"}\n"
            + "{\"id\":\"2\",\"age\":24,\"gender\":\"N\"}\n"
            + "{\"id\":\"3\",\"age\":36,\"gender\":\"M\"}",
        false);

    verify(jobService, times(1))
        .newJob(any(), eq(DatasetScratchFilePrepareFlight.class), any(), any());
    verify(jobService, times(1))
        .newJob(any(), eq(DatasetIngestFlight.class), requestCaptor.capture(), any());
    assertThat("payload is stripped out", requestCaptor.getValue().getRecords(), empty());
  }

  @Test
  public void ingestPayloadDataAzure() throws Exception {
    UUID datasetId = createDataset("dataset-create-test.json", CloudPlatform.AZURE);
    String filePath = "foopath";
    String signedPath = "foopathsigned";
    AzureStorageAccountResource storageAccountResource = mock(AzureStorageAccountResource.class);
    AzureApplicationDeploymentResource applicationResource =
        mock(AzureApplicationDeploymentResource.class);
    when(applicationResource.getAzureResourceGroupName()).thenReturn("mrg");
    BlobClient blobClient = mock(BlobClient.class);
    when(blobClient.getBlobUrl()).thenReturn(filePath);
    BlobContainerClient containerClient = mock(BlobContainerClient.class);
    when(containerClient.getBlobClient(any())).thenReturn(blobClient);
    when(storageAccountResource.getApplicationResource()).thenReturn(applicationResource);
    when(resourceService.getOrCreateDatasetStorageAccount(any(), any(), any()))
        .thenReturn(storageAccountResource);
    // Mock that the monitoring stack already exists so creation steps are skipped
    when(azureMonitoringService.getLogAnalyticsWorkspace(any(), any()))
        .thenReturn(mock(Workspace.class));
    when(azureMonitoringService.getDiagnosticSetting(any(), any()))
        .thenReturn(mock(DiagnosticSetting.class));
    when(azureContainerPdao.getContainer(any(), any())).thenReturn(containerClient);
    when(azureContainerPdao.getOrCreateContainer(any(), any())).thenReturn(containerClient);
    when(azureBlobStorePdao.signFile(any(), eq(storageAccountResource), eq(filePath), any()))
        .thenReturn(signedPath);
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel()
            .loadTag("lt")
            .format(FormatEnum.ARRAY)
            .updateStrategy(UpdateStrategyEnum.APPEND)
            .table("participant")
            .addRecordsItem(Map.of("id", "1", "age", 12, "gender", "F"))
            .addRecordsItem(Map.of("id", "2", "age", 24, "gender", "N"))
            .addRecordsItem(Map.of("id", "3", "age", 36, "gender", "M"));

    datasetService.ingestDataset(datasetId.toString(), ingestRequestModel, testUser);

    verify(azureBlobStorePdao, times(1)).writeBlobLines(any(), listCaptor.capture());

    JSONAssert.assertEquals(
        "correct lines were written",
        String.join("\n", listCaptor.getValue()),
        "{\"id\":\"1\",\"age\":12,\"gender\":\"F\"}\n"
            + "{\"id\":\"2\",\"age\":24,\"gender\":\"N\"}\n"
            + "{\"id\":\"3\",\"age\":36,\"gender\":\"M\"}",
        false);

    verify(jobService, times(1))
        .newJob(any(), eq(DatasetScratchFilePrepareFlight.class), any(), any());
    verify(jobService, times(1))
        .newJob(any(), eq(DatasetIngestFlight.class), requestCaptor.capture(), any());
    assertThat("payload is stripped out", requestCaptor.getValue().getRecords(), empty());
  }

  @Test
  public void getOrCreateExternalAzureDataSourceHidesExceptionInformation() throws Exception {
    UUID datasetId = UUID.randomUUID();
    Dataset dataset = new Dataset().id(datasetId);
    when(metadataDataAccessUtils.accessInfoFromDataset(dataset, testUser))
        .thenReturn(
            new AccessInfoModel()
                .parquet(
                    new AccessInfoParquetModel()
                        .sasToken(
                            "sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04&sr=b&sig=mysig")
                        .url("https://fake.url")));
    doThrow(SQLException.class)
        .when(azureSynapsePdao)
        .getOrCreateExternalDataSourceForResource(
            any(AccessInfoModel.class), any(UUID.class), eq(testUser));

    assertError(
        RuntimeException.class,
        "Could not configure external datasource",
        () -> datasetService.getOrCreateExternalAzureDataSource(dataset, testUser));
  }
}
