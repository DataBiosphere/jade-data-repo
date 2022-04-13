package bio.terra.service.snapshot;

import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.logging.v2.LifecycleState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
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
public class SnapshotConnectedTest {
  // TODO: MORE TESTS to be done when we can ingest data:
  // - test more complex datasets with relationships
  // - test relationship walking with valid and invalid setups
  // TODO: MORE TESTS when we separate the value translation from the create
  // - test invalid row ids

  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private DatasetDao datasetDao;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ConfigurationService configService;
  @Autowired private DrsIdService drsIdService;
  @Autowired private GoogleResourceManagerService googleResourceManagerService;

  @MockBean private IamProviderInterface samService;

  private String snapshotOriginalName;
  private BillingProfileModel billingProfile;
  private final Storage storage = StorageOptions.getDefaultInstance().getService();
  private DatasetSummaryModel datasetSummary;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    datasetSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "snapshot-test-dataset.json");
    SnapshotConnectedTestUtils.loadCsvData(
        connectedOperations,
        jsonLoader,
        storage,
        testConfig.getIngestbucket(),
        datasetSummary.getId(),
        "thetable",
        "snapshot-test-dataset-data-row-ids.csv");
  }

  @After
  public void tearDown() throws Exception {
    connectedOperations.teardown();
    configService.reset();
  }

  @Test
  public void testArrayStruct() throws Exception {
    DatasetSummaryModel datasetArraySummary = setupArrayStructDataset();
    String datasetName = PDAO_PREFIX + datasetArraySummary.getName();
    BigQueryProject bigQueryProject =
        TestUtils.bigQueryProjectForDatasetName(datasetDao, datasetArraySummary.getName());
    long datasetParticipants =
        SnapshotConnectedTestUtils.queryForCount(datasetName, "participant", bigQueryProject);
    assertThat("dataset participants loaded properly", datasetParticipants, equalTo(2L));
    long datasetSamples =
        SnapshotConnectedTestUtils.queryForCount(datasetName, "sample", bigQueryProject);
    assertThat("dataset samples loaded properly", datasetSamples, equalTo(5L));

    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetArraySummary, "snapshot-array-struct.json");
    MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "");
    SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);
    SnapshotConnectedTestUtils.getTestSnapshot(
        mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetArraySummary);

    BigQueryProject bigQuerySnaphsotProject =
        TestUtils.bigQueryProjectForSnapshotName(snapshotDao, summaryModel.getName());
    long snapshotParticipants =
        SnapshotConnectedTestUtils.queryForCount(
            summaryModel.getName(), "participant", bigQuerySnaphsotProject);
    assertThat("dataset participants loaded properly", snapshotParticipants, equalTo(2L));
    long snapshotSamples =
        SnapshotConnectedTestUtils.queryForCount(
            summaryModel.getName(), "sample", bigQuerySnaphsotProject);
    assertThat("dataset samples loaded properly", snapshotSamples, equalTo(3L));
  }

  @Test
  public void testEnumeration() throws Exception {
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetSummary, "snapshot-test-snapshot.json");

    // Other unit tests exercise the array bounds, so here we don't fuss with that here.
    // Just make sure we get the same snapshot summary that we made.
    List<SnapshotSummaryModel> snapshotList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_en_");
      SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);
      snapshotList.add(summaryModel);
    }

    // Reverse the order of the array since the order we return the snapshots in by default is
    // descending order of creation
    Collections.reverse(snapshotList);

    Map<UUID, Set<IamRole>> snapshotIds =
        snapshotList.stream()
            .map(SnapshotSummaryModel::getId)
            .collect(Collectors.toMap(Function.identity(), x -> Set.of(IamRole.READER)));

    when(samService.listAuthorizedResources(any(), any())).thenReturn(snapshotIds);
    EnumerateSnapshotModel enumResponse = enumerateTestSnapshots();
    List<SnapshotSummaryModel> enumeratedArray = enumResponse.getItems();
    assertThat("total is correct", enumResponse.getTotal(), equalTo(5));

    // The enumeratedArray may contain more snapshots than just the set we created,
    // but ours should be in order in the enumeration. So we do a merge waiting until we match
    // by id and then comparing contents.
    int compareIndex = 0;
    for (SnapshotSummaryModel anEnumeratedSnapshot : enumeratedArray) {
      if (anEnumeratedSnapshot.getId().equals(snapshotList.get(compareIndex).getId())) {
        assertThat(
            "MetadataEnumeration summary matches create summary",
            anEnumeratedSnapshot,
            equalTo(snapshotList.get(compareIndex)));
        compareIndex++;
      }
    }

    assertThat("we found all snapshots", compareIndex, equalTo(5));

    for (int i = 0; i < 5; i++) {
      connectedOperations.deleteTestSnapshot(enumeratedArray.get(i).getId());
    }
  }

  @Test
  public void testBadData() throws Exception {
    SnapshotRequestModel badDataRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetSummary, "snapshot-test-snapshot-baddata.json");

    MockHttpServletResponse response = performCreateSnapshot(badDataRequest, "_baddata_");
    ErrorModel errorModel = handleCreateSnapshotFailureCase(response);
    assertThat(errorModel.getMessage(), containsString("Fred"));
  }

  @Test
  public void testDuplicateName() throws Exception {
    // create a snapshot
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetSummary, "snapshot-test-snapshot.json");
    MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_dup_");
    SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

    // fetch the snapshot and confirm the metadata matches the request
    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetSummary);
    assertNotNull("fetched snapshot successfully after creation", snapshotModel);

    // check that the snapshot metadata row is unlocked
    String exclusiveLock = snapshotDao.getExclusiveLockState(snapshotModel.getId());
    assertNull("snapshot row is unlocked", exclusiveLock);

    // try to create the same snapshot again and check that it fails
    snapshotRequest.setName(snapshotModel.getName());
    response = performCreateSnapshot(snapshotRequest, null);
    ErrorModel errorModel = handleCreateSnapshotFailureCase(response);
    assertThat(response.getStatus(), equalTo(HttpStatus.BAD_REQUEST.value()));
    assertThat(
        "error message includes name conflict",
        errorModel.getMessage(),
        containsString("Snapshot name or id already exists"));

    // fetch the snapshot and confirm the metadata still matches the original
    SnapshotModel origModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetSummary);
    assertEquals("fetched snapshot remains unchanged", snapshotModel, origModel);

    // delete and confirm deleted
    connectedOperations.deleteTestSnapshot(snapshotModel.getId());
    connectedOperations.getSnapshotExpectError(snapshotModel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testDeleteRecreateSnapshot() throws Exception {
    // create a dataset and load some tabular data
    DatasetSummaryModel datasetSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "snapshot-test-dataset.json");
    SnapshotConnectedTestUtils.loadCsvData(
        connectedOperations,
        jsonLoader,
        storage,
        testConfig.getIngestbucket(),
        datasetSummary.getId(),
        "thetable",
        "snapshot-test-dataset-data-row-ids.csv");

    // create a snapshot
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetSummary, "snapshot-test-snapshot.json");
    MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_dup_");
    SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

    // fetch the snapshot and confirm the metadata matches the request
    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetSummary);
    assertNotNull("fetched snapshot successfully after creation", snapshotModel);

    // check that the snapshot metadata row is unlocked
    String exclusiveLock = snapshotDao.getExclusiveLockState(snapshotModel.getId());
    assertNull("snapshot row is unlocked", exclusiveLock);

    // delete and confirm deleted
    connectedOperations.deleteTestSnapshot(snapshotModel.getId());
    connectedOperations.getSnapshotExpectError(snapshotModel.getId(), HttpStatus.NOT_FOUND);

    // now after deleting the snapshot, make sure you can create it again and the delete worked!
    snapshotRequest.setName(snapshotModel.getName());
    response = performCreateSnapshot(snapshotRequest, null);
    SnapshotSummaryModel summaryModelSequel = validateSnapshotCreated(snapshotRequest, response);

    // then delete it a final time for cleanup
    connectedOperations.deleteTestSnapshot(summaryModelSequel.getId());
    connectedOperations.getSnapshotExpectError(summaryModelSequel.getId(), HttpStatus.NOT_FOUND);
  }

  @Test
  public void testProjectDeleteAfterSnapshotDelete() throws Exception {
    // create a dataset and load some tabular data
    DatasetSummaryModel datasetSummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "snapshot-test-dataset.json");
    SnapshotConnectedTestUtils.loadCsvData(
        connectedOperations,
        jsonLoader,
        storage,
        testConfig.getIngestbucket(),
        datasetSummary.getId(),
        "thetable",
        "snapshot-test-dataset-data-row-ids.csv");

    // create a snapshot
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader, datasetSummary, "snapshot-test-snapshot.json");
    MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_dup_");
    SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

    // retrieve snapshot and store project id
    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetSummary);
    assertNotNull("fetched snapshot successfully after creation", snapshotModel);
    String googleProjectId = snapshotModel.getDataProject();

    // ensure that google project exists
    Assert.assertNotNull(googleResourceManagerService.getProject(googleProjectId));

    // delete snapshot
    connectedOperations.deleteTestSnapshot(snapshotModel.getId());
    connectedOperations.getSnapshotExpectError(snapshotModel.getId(), HttpStatus.NOT_FOUND);

    // check that google project doesn't exist
    Assert.assertEquals(
        LifecycleState.DELETE_REQUESTED.toString(),
        googleResourceManagerService.getProject(googleProjectId).getLifecycleState());
  }

  @Ignore("Remove ignore after DR-1770 is addressed")
  @Test
  public void testOverlappingDeletes() throws Exception {
    // create a snapshot
    SnapshotSummaryModel summaryModel =
        connectedOperations.createSnapshot(datasetSummary, "snapshot-test-snapshot.json", "_d2_");

    // NO ASSERTS inside the block below where hang is enabled to reduce chance of failing before
    // disabling the hang
    // ====================================================
    // enable hang in DeleteSnapshotPrimaryDataStep
    configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT.name(), true);

    // try to delete the snapshot
    MvcResult result1 =
        mvc.perform(delete("/api/repository/v1/snapshots/" + summaryModel.getId())).andReturn();

    // try to delete the snapshot again, this should fail with a lock exception
    // note: asserts are below outside the hang block
    MvcResult result2 =
        mvc.perform(delete("/api/repository/v1/snapshots/" + summaryModel.getId())).andReturn();

    // disable hang in DeleteSnapshotPrimaryDataStep
    configService.setFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
    // ====================================================

    // check the response from the first delete request
    MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
    DeleteResponseModel deleteResponseModel =
        connectedOperations.handleSuccessCase(response1, DeleteResponseModel.class);
    assertEquals(
        "First delete returned successfully",
        DeleteResponseModel.ObjectStateEnum.DELETED,
        deleteResponseModel.getObjectState());

    // check the response from the second delete request
    MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);
    ErrorModel errorModel2 =
        connectedOperations.handleFailureCase(response2, HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(
        "delete failed on lock exception",
        errorModel2.getMessage(),
        startsWith("Failed to lock the snapshot"));

    // confirm deleted
    connectedOperations.getSnapshotExpectError(summaryModel.getId(), HttpStatus.NOT_FOUND);
  }

  private DatasetSummaryModel setupArrayStructDataset() throws Exception {
    DatasetSummaryModel datasetArraySummary =
        SnapshotConnectedTestUtils.createTestDataset(
            connectedOperations, billingProfile, "dataset-array-struct.json");
    loadJsonData(
        datasetArraySummary.getId(), "participant", "dataset-array-struct-participant.json");
    loadJsonData(datasetArraySummary.getId(), "sample", "dataset-array-struct-sample.json");
    return datasetArraySummary;
  }

  private void loadJsonData(UUID datasetId, String tableName, String resourcePath)
      throws Exception {
    SnapshotConnectedTestUtils.loadData(
        connectedOperations,
        jsonLoader,
        storage,
        testConfig.getIngestbucket(),
        datasetId,
        tableName,
        resourcePath,
        IngestRequestModel.FormatEnum.JSON);
  }

  private MockHttpServletResponse performCreateSnapshot(
      SnapshotRequestModel snapshotRequest, String infix) throws Exception {
    snapshotOriginalName = snapshotRequest.getName();
    MvcResult result = SnapshotConnectedTestUtils.launchCreateSnapshot(mvc, snapshotRequest, infix);
    MockHttpServletResponse response = connectedOperations.validateJobModelAndWait(result);
    return response;
  }

  private SnapshotSummaryModel validateSnapshotCreated(
      SnapshotRequestModel snapshotRequest, MockHttpServletResponse response) throws Exception {
    SnapshotSummaryModel summaryModel =
        connectedOperations.handleCreateSnapshotSuccessCase(response);

    assertThat(summaryModel.getDescription(), equalTo(snapshotRequest.getDescription()));
    assertThat(summaryModel.getName(), equalTo(snapshotRequest.getName()));

    // Reset the name in the snapshot request
    snapshotRequest.setName(snapshotOriginalName);

    return summaryModel;
  }

  private ErrorModel handleCreateSnapshotFailureCase(MockHttpServletResponse response)
      throws Exception {
    String responseBody = response.getContentAsString();
    HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
    assertFalse("Expect create snapshot failure", responseStatus.is2xxSuccessful());

    assertTrue(
        "Error model was returned on failure", StringUtils.contains(responseBody, "message"));

    return TestUtils.mapFromJson(responseBody, ErrorModel.class);
  }

  private EnumerateSnapshotModel enumerateTestSnapshots() throws Exception {
    MvcResult result =
        mvc.perform(get("/api/repository/v1/snapshots?offset=0&limit=1000"))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    EnumerateSnapshotModel summary =
        TestUtils.mapFromJson(response.getContentAsString(), EnumerateSnapshotModel.class);
    return summary;
  }
}
