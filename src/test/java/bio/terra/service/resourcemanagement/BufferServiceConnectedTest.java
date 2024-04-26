package bio.terra.service.resourcemanagement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.ResourceId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
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
public class BufferServiceConnectedTest {

  @Autowired private BufferService bufferService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private MockMvc mvc;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ConfigurationService configService;
  @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
  @Autowired private GoogleResourceManagerService resourceManagerService;

  @MockBean private IamProviderInterface samService;

  private Storage storage = StorageOptions.getDefaultInstance().getService();
  private BillingProfileModel billingProfile;

  private List<String> googleProjectIds = new ArrayList<>();

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
  }

  @After
  public void tearDown() throws Exception {
    for (String googleProjectId : googleProjectIds) {
      resourceManagerService.cloudResourceManager().projects().delete(googleProjectId).execute();
    }
    connectedOperations.teardown();
    configService.reset();
  }

  @Test
  public void testProjectHandout() {
    ResourceInfo resource = bufferService.handoutResource(false);
    String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    Project project = resourceManagerService.getProject(projectId);
    resourceManagerService.addLabelsToProject(projectId, Map.of("test-name", "rbs-connected-test"));
    assertThat(
        "The project requested from RBS is active", project.getLifecycleState(), equalTo("ACTIVE"));
  }

  @Test
  public void twoDatasetsDifferentProjects() throws Exception {
    DatasetSummaryModel summaryModel1 = setupMinimalDataset();
    DatasetModel dataset1 = connectedOperations.getDataset(summaryModel1.getId());

    DatasetSummaryModel summaryModel2 = setupMinimalDataset();
    DatasetModel dataset2 = connectedOperations.getDataset(summaryModel2.getId());

    assertThat(
        "Two datasets with the same billing profile get different projects",
        dataset1.getDataProject(),
        not(dataset2.getDataProject()));
  }

  @Test
  public void testDatasetAndSnapshotSeparateProjects() throws Exception {
    DatasetSummaryModel datasetMinimalSummary = setupMinimalDataset();

    DatasetModel dataset = connectedOperations.getDataset(datasetMinimalSummary.getId());

    SnapshotRequestModel snapshotRequest =
        makeSnapshotTestRequest(datasetMinimalSummary, "dataset-minimal-snapshot.json");
    SnapshotSummaryModel summaryModel = performCreateSnapshot(snapshotRequest, "");

    SnapshotModel snapshotModel = getTestSnapshot(summaryModel.getId());

    assertThat(
        "Dataset and Snapshot have separate projects from RBS",
        dataset.getDataProject(),
        not(snapshotModel.getDataProject()));
  }

  @Test
  public void testProjectRefoldering() throws Exception {
    ResourceInfo sensitiveResource = bufferService.handoutResource(true);
    String sensitiveProjectId =
        sensitiveResource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    googleProjectIds.add(sensitiveProjectId);
    Project sensitiveProject = resourceManagerService.getProject(sensitiveProjectId);
    ResourceId sensitiveParent = sensitiveProject.getParent();

    ResourceInfo normalResource = bufferService.handoutResource(false);
    String normalProjectId =
        normalResource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    googleProjectIds.add(normalProjectId);
    Project normalProject = resourceManagerService.getProject(normalProjectId);
    ResourceId normalParent = normalProject.getParent();

    assertThat(
        "Sensitive project is in a different folder than non-sensitive project",
        sensitiveParent.getId(),
        not(normalParent.getId()));

    assertThat(
        "Sensitive project was moved to the correct folder",
        sensitiveParent.getId(),
        equalTo(googleResourceConfiguration.secureFolderResourceId()));

    // test trying to move a project that is already in the secure folder
    bufferService.refolderProjectToSecureFolder(sensitiveProjectId);
    assertThat(
        "Sensitive project is still in secure folder",
        sensitiveParent.getId(),
        equalTo(googleResourceConfiguration.secureFolderResourceId()));

    // test trying to move a project back to the default folder
    bufferService.refolderProjectToDefaultFolder(sensitiveProjectId);
    // Reload the project to get the updated parent folder
    ResourceId updatedParentFolder =
        resourceManagerService.getProject(sensitiveProjectId).getParent();
    assertThat(
        "Formerly sensitive project is now in default folder",
        updatedParentFolder.getId(),
        equalTo(normalParent.getId()));

    // test trying to move a project back to the default folder
    bufferService.refolderProjectToDefaultFolder(sensitiveProjectId);
    assertThat(
        "Formerly sensitive project is still in default folder",
        updatedParentFolder.getId(),
        equalTo(normalParent.getId()));
  }

  private SnapshotModel getTestSnapshot(UUID id) throws Exception {
    MvcResult result =
        mvc.perform(get("/api/repository/v1/snapshots/" + id))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    return objectMapper.readValue(response.getContentAsString(), SnapshotModel.class);
  }

  private DatasetSummaryModel setupMinimalDataset() throws Exception {
    DatasetSummaryModel datasetMinimalSummary =
        connectedOperations.createDataset(billingProfile, DaoOperations.DATASET_MINIMAL);
    loadData(
        datasetMinimalSummary.getId(),
        "participant",
        "dataset-minimal-participant.csv",
        IngestRequestModel.FormatEnum.CSV);
    loadData(
        datasetMinimalSummary.getId(),
        "sample",
        "dataset-minimal-sample.csv",
        IngestRequestModel.FormatEnum.CSV);
    return datasetMinimalSummary;
  }

  private void loadData(
      UUID datasetId, String tableName, String resourcePath, IngestRequestModel.FormatEnum format)
      throws Exception {

    String bucket = testConfig.getIngestbucket();
    BlobInfo stagingBlob =
        BlobInfo.newBuilder(bucket, UUID.randomUUID() + "-" + resourcePath).build();
    byte[] data = IOUtils.toByteArray(jsonLoader.getClassLoader().getResource(resourcePath));

    IngestRequestModel ingestRequest =
        new IngestRequestModel()
            .table(tableName)
            .format(format)
            .path("gs://" + stagingBlob.getBucket() + "/" + stagingBlob.getName());

    if (format.equals(IngestRequestModel.FormatEnum.CSV)) {
      ingestRequest.csvSkipLeadingRows(1);
      ingestRequest.csvGenerateRowIds(false);
    }

    try {
      storage.create(stagingBlob, data);
      connectedOperations.ingestTableSuccess(datasetId, ingestRequest);
    } finally {
      storage.delete(stagingBlob.getBlobId());
    }
  }

  private SnapshotRequestModel makeSnapshotTestRequest(
      DatasetSummaryModel datasetSummaryModel, String resourcePath) throws Exception {
    SnapshotRequestModel snapshotRequest =
        jsonLoader.loadObject(resourcePath, SnapshotRequestModel.class);
    SnapshotRequestContentsModel content = snapshotRequest.getContents().get(0);
    // TODO SingleDatasetSnapshot
    String newDatasetName = datasetSummaryModel.getName();
    String origDatasetName = content.getDatasetName();
    // swap in the correct dataset name (with the id at the end)
    content.setDatasetName(newDatasetName);
    snapshotRequest.profileId(datasetSummaryModel.getDefaultProfileId());
    if (content.getMode().equals(SnapshotRequestContentsModel.ModeEnum.BYQUERY)) {
      // if its by query, also set swap in the correct dataset name in the query
      String query = content.getQuerySpec().getQuery();
      content.getQuerySpec().setQuery(query.replace(origDatasetName, newDatasetName));
    }
    return snapshotRequest;
  }

  private MvcResult launchCreateSnapshot(SnapshotRequestModel snapshotRequest, String infix)
      throws Exception {
    if (infix != null) {
      String snapshotName = Names.randomizeNameInfix(snapshotRequest.getName(), infix);
      snapshotRequest.setName(snapshotName);
    }

    String jsonRequest = TestUtils.mapToJson(snapshotRequest);

    return mvc.perform(
            post("/api/repository/v1/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonRequest))
        .andExpect(content().contentType(MediaType.APPLICATION_JSON))
        .andReturn();
  }

  private SnapshotSummaryModel performCreateSnapshot(
      SnapshotRequestModel snapshotRequest, String infix) throws Exception {
    MvcResult result = launchCreateSnapshot(snapshotRequest, infix);
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);
    SnapshotSummaryModel summaryModel =
        connectedOperations.handleCreateSnapshotSuccessCase(response);
    return summaryModel;
  }
}
