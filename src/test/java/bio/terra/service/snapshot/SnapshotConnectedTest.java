package bio.terra.service.snapshot;

import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.ResourceLocksUtils;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.ras.EcmService;
import bio.terra.service.auth.ras.RasDbgapPermissions;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.cloudresourcemanager.model.Project;
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
import org.junit.Before;
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
  @MockBean private EcmService ecmService;

  private String snapshotOriginalName;
  private BillingProfileModel billingProfile;
  private final Storage storage = StorageOptions.getDefaultInstance().getService();
  private DatasetSummaryModel datasetSummary;

  private static final String CONSENT_CODE = "c99";
  private static final String PHS_ID = "phs123456";

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    when(ecmService.getRasDbgapPermissions(any()))
        .thenReturn(List.of(new RasDbgapPermissions(CONSENT_CODE, PHS_ID)));
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
            jsonLoader,
            datasetArraySummary,
            "snapshot-array-struct.json",
            datasetArraySummary.getDefaultProfileId());
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
    datasetDao.patch(
        datasetSummary.getId(), new DatasetPatchRequestModel().phsId(PHS_ID), TEST_USER);

    // Other unit tests exercise the array bounds, so here we don't fuss with that here.
    // Just make sure we get the same snapshot summary that we made.
    List<SnapshotSummaryModel> snapshotList = new ArrayList<>();

    // A snapshot is authorized to be included in enumeration if the caller can access it directly
    // via SAM and/or indirectly via a linked RAS passport.
    // Create three snapshots with consent code -- authorized indirectly via a linked RAS passport.
    SnapshotRequestModel rasSnapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
                jsonLoader,
                datasetSummary,
                "snapshot-test-snapshot.json",
                datasetSummary.getDefaultProfileId())
            .consentCode(CONSENT_CODE);
    List<UUID> rasSnapshotIds = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      MockHttpServletResponse response = performCreateSnapshot(rasSnapshotRequest, "_en_");
      SnapshotSummaryModel summaryModel = validateSnapshotCreated(rasSnapshotRequest, response);
      snapshotList.add(summaryModel);
      rasSnapshotIds.add(summaryModel.getId());
    }
    assertThat("3 RAS-authorized snapshots", rasSnapshotIds, hasSize(3));

    // Create two snapshots without consent code -- not authorized via a linked RAS passport.
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader,
            datasetSummary,
            "snapshot-test-snapshot.json",
            datasetSummary.getDefaultProfileId());
    List<UUID> samSnapshotIds = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_en_");
      SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);
      snapshotList.add(summaryModel);
      samSnapshotIds.add(summaryModel.getId());
    }

    // Designate one of our snapshots as accessible via SAM *and* a linked RAS passport.
    samSnapshotIds.add(rasSnapshotIds.get(0));
    assertThat("3 SAM-authorized snapshots", samSnapshotIds, hasSize(3));

    IamRole samIamRole = IamRole.STEWARD;
    Map<UUID, Set<IamRole>> samIdsAndRoles =
        samSnapshotIds.stream()
            .collect(Collectors.toMap(Function.identity(), x -> Set.of(samIamRole)));
    when(samService.listAuthorizedResources(any(), any())).thenReturn(samIdsAndRoles);

    assertThat("5 total snapshots created", snapshotList, hasSize(5));

    // Reverse the order of the array since the order we return the snapshots in by default is
    // descending order of creation
    Collections.reverse(snapshotList);

    EnumerateSnapshotModel enumResponse = enumerateTestSnapshots();
    List<SnapshotSummaryModel> enumeratedArray = enumResponse.getItems();
    assertThat("total is correct", enumResponse.getTotal(), equalTo(5));

    // The enumeratedArray may contain more snapshots than just the set we created,
    // but ours should be in order in the enumeration. So we do a merge waiting until we match
    // by id and then comparing contents.
    int compareIndex = 0;

    for (SnapshotSummaryModel anEnumeratedSnapshot : enumeratedArray) {
      UUID actualId = anEnumeratedSnapshot.getId();
      if (actualId.equals(snapshotList.get(compareIndex).getId())) {
        assertThat(
            "MetadataEnumeration summary matches create summary",
            anEnumeratedSnapshot,
            equalTo(snapshotList.get(compareIndex)));

        List<String> expectedIamRoles = new ArrayList<>();
        if (samSnapshotIds.contains(actualId)) {
          expectedIamRoles.add(samIamRole.toString());
        }
        if (rasSnapshotIds.contains(actualId)) {
          expectedIamRoles.add(IamRole.READER.toString());
        }

        assertThat(
            "IAM roles as expected given snapshot accessibility",
            enumResponse.getRoleMap().get(actualId.toString()),
            containsInAnyOrder(expectedIamRoles.toArray()));

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
            jsonLoader,
            datasetSummary,
            "snapshot-test-snapshot-baddata.json",
            datasetSummary.getDefaultProfileId());

    MockHttpServletResponse response = performCreateSnapshot(badDataRequest, "_baddata_");
    ErrorModel errorModel = handleCreateSnapshotFailureCase(response);
    assertThat(errorModel.getMessage(), containsString("Fred"));
  }

  @Test
  public void testDuplicateName() throws Exception {
    // create a snapshot
    SnapshotRequestModel snapshotRequest =
        SnapshotConnectedTestUtils.makeSnapshotTestRequest(
            jsonLoader,
            datasetSummary,
            "snapshot-test-snapshot.json",
            datasetSummary.getDefaultProfileId());
    MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_dup_");
    SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

    // fetch the snapshot and confirm the metadata matches the request
    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetSummary);
    assertNotNull("fetched snapshot successfully after creation", snapshotModel);

    // check that the snapshot metadata row is unlocked
    assertNull(
        "snapshot row is unlocked",
        ResourceLocksUtils.getExclusiveLock(snapshotModel.getResourceLocks()));

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
            jsonLoader,
            datasetSummary,
            "snapshot-test-snapshot.json",
            datasetSummary.getDefaultProfileId());
    MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_dup_");
    SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

    // fetch the snapshot and confirm the metadata matches the request
    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetSummary);
    assertNotNull("fetched snapshot successfully after creation", snapshotModel);

    // check that the snapshot metadata row is unlocked
    assertNull(
        "snapshot row is unlocked",
        ResourceLocksUtils.getExclusiveLock(snapshotModel.getResourceLocks()));

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
            jsonLoader,
            datasetSummary,
            "snapshot-test-snapshot.json",
            datasetSummary.getDefaultProfileId());
    MockHttpServletResponse response = performCreateSnapshot(snapshotRequest, "_dup_");
    SnapshotSummaryModel summaryModel = validateSnapshotCreated(snapshotRequest, response);

    // retrieve snapshot and store project id
    SnapshotModel snapshotModel =
        SnapshotConnectedTestUtils.getTestSnapshot(
            mvc, objectMapper, summaryModel.getId(), snapshotRequest, datasetSummary);
    assertNotNull("fetched snapshot successfully after creation", snapshotModel);
    String googleProjectId = snapshotModel.getDataProject();

    // ensure that google project exists
    Project project = googleResourceManagerService.getProject(googleProjectId);
    assertNotNull(project);

    // Ensure that the name is correct
    assertEquals("TDR Snapshot Project", project.getName());

    // delete snapshot
    connectedOperations.deleteTestSnapshot(snapshotModel.getId());
    connectedOperations.getSnapshotExpectError(snapshotModel.getId(), HttpStatus.NOT_FOUND);

    // check that google project doesn't exist
    assertEquals(
        LifecycleState.DELETE_REQUESTED.toString(),
        googleResourceManagerService.getProject(googleProjectId).getLifecycleState());
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
