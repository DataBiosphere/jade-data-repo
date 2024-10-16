package bio.terra.app.controller;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.SqlSortDirection;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.model.QueryDataRequestModel;
import bio.terra.model.ResourceLocks;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderConceptsResponse;
import bio.terra.model.SnapshotBuilderCountRequest;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
import bio.terra.model.SnapshotBuilderGetConceptHierarchyResponse;
import bio.terra.model.SnapshotBuilderParentConcept;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.SqlSortDirectionAscDefault;
import bio.terra.model.UnlockResourceRequest;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(classes = {SnapshotsApiController.class, GlobalExceptionHandler.class})
@Tag(Unit.TAG)
@WebMvcTest
class SnapshotsApiControllerTest {

  @Autowired private MockMvc mvc;

  @MockBean private JobService jobService;
  @MockBean private SnapshotRequestValidator snapshotRequestValidator;
  @MockBean private SnapshotService snapshotService;
  @MockBean private IamService iamService;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private FileService fileService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private AssetModelValidator assetModelValidator;
  @MockBean private SnapshotBuilderService snapshotBuilderService;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final String JOB_ID = "a-job-id";
  private static final JobModel JOB_MODEL =
      new JobModel().id(JOB_ID).jobStatus(JobModel.JobStatusEnum.RUNNING);
  private static final SnapshotRequestModel SNAPSHOT_REQUEST_MODEL =
      new SnapshotRequestModel().name("snapshot_name");
  private static final Boolean EXPORT_GCS_PATHS = false;
  private static final Boolean VALIDATE_PRIMARY_KEY_UNIQUENESS = true;
  private static final Boolean SIGN_URLS = true;
  private static final String TABLE_NAME = "table1";
  private static final String COLUMN_NAME = PDAO_ROW_ID_COLUMN;
  private static final int LIMIT = 100;
  private static final int OFFSET = 0;
  private static final EnumerateSortByParam SORT = EnumerateSortByParam.NAME;
  private static final SqlSortDirectionAscDefault DIRECTION = SqlSortDirectionAscDefault.ASC;
  private static final String FILTER = "";
  private static final String REGION = "a-region";
  private static final String TAG = "a-tag";
  private static final String DUOS_ID = "a-duos-id";
  private static final SnapshotRetrieveIncludeModel INCLUDE = SnapshotRetrieveIncludeModel.TABLES;
  private static final String SNAPSHOTS_ENDPOINT = "/api/repository/v1/snapshots";
  private static final String SNAPSHOT_ID_ENDPOINT = SNAPSHOTS_ENDPOINT + "/{id}";
  private static final String LOCK_SNAPSHOT_ENDPOINT = SNAPSHOT_ID_ENDPOINT + "/lock";
  private static final String UNLOCK_SNAPSHOT_ENDPOINT = SNAPSHOT_ID_ENDPOINT + "/unlock";
  private static final String QUERY_SNAPSHOT_DATA_ENDPOINT = SNAPSHOT_ID_ENDPOINT + "/data/{table}";
  private static final String EXPORT_SNAPSHOT_ENDPOINT = SNAPSHOT_ID_ENDPOINT + "/export";
  private static final String SNAPSHOT_BUILDER_SETTINGS_ENDPOINT =
      SNAPSHOT_ID_ENDPOINT + "/snapshotBuilder/settings";

  private static final String SNAPSHOT_BUILDER_ENDPOINT = SNAPSHOT_ID_ENDPOINT + "/snapshotBuilder";
  private static final String GET_CONCEPT_CHILDREN_ENDPOINT =
      SNAPSHOT_BUILDER_ENDPOINT + "/concepts/{conceptId}/children";
  private static final String GET_COUNT_ENDPOINT = SNAPSHOT_BUILDER_ENDPOINT + "/count";
  private static final String ENUMERATE_CONCEPTS_ENDPOINT = SNAPSHOT_BUILDER_ENDPOINT + "/concepts";
  private static final String GET_CONCEPT_HIERARCHY_ENDPOINT =
      SNAPSHOT_BUILDER_ENDPOINT + "/concepts/{conceptId}/hierarchy";
  private static final Integer CONCEPT_ID = 0;

  @BeforeEach
  void setUp() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void testExportSnapshot() throws Exception {
    when(snapshotService.exportSnapshot(
            SNAPSHOT_ID, TEST_USER, EXPORT_GCS_PATHS, VALIDATE_PRIMARY_KEY_UNIQUENESS, SIGN_URLS))
        .thenReturn(JOB_ID);
    when(jobService.retrieveJob(JOB_ID, TEST_USER)).thenReturn(JOB_MODEL);

    String actualJson =
        mvc.perform(
                get(EXPORT_SNAPSHOT_ENDPOINT, SNAPSHOT_ID)
                    .queryParam("exportGsPaths", String.valueOf(EXPORT_GCS_PATHS))
                    .queryParam(
                        "validatePrimaryKeyUniqueness",
                        String.valueOf(VALIDATE_PRIMARY_KEY_UNIQUENESS)))
            .andExpect(status().isAccepted())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JobModel actual = TestUtils.mapFromJson(actualJson, JobModel.class);
    assertThat("Job model is returned", actual, equalTo(JOB_MODEL));

    verifyAuthorizationCall(IamAction.EXPORT_SNAPSHOT);
    verify(snapshotService)
        .exportSnapshot(
            SNAPSHOT_ID, TEST_USER, EXPORT_GCS_PATHS, VALIDATE_PRIMARY_KEY_UNIQUENESS, SIGN_URLS);
  }

  @Test
  void testExportSnapshotNotFound() throws Exception {
    doThrow(SnapshotNotFoundException.class)
        .when(snapshotService)
        .retrieveSnapshotSummary(SNAPSHOT_ID);
    mvc.perform(
            get(EXPORT_SNAPSHOT_ENDPOINT, SNAPSHOT_ID)
                .queryParam("exportGsPaths", String.valueOf(EXPORT_GCS_PATHS))
                .queryParam(
                    "validatePrimaryKeyUniqueness",
                    String.valueOf(VALIDATE_PRIMARY_KEY_UNIQUENESS)))
        .andExpect(status().isNotFound());
    verifyNoInteractions(iamService);
  }

  @Test
  void testExportSnapshotForbidden() throws Exception {
    IamAction iamAction = IamAction.EXPORT_SNAPSHOT;
    failValidation(iamAction);

    mvc.perform(
            get(EXPORT_SNAPSHOT_ENDPOINT, SNAPSHOT_ID)
                .queryParam("exportGsPaths", String.valueOf(EXPORT_GCS_PATHS))
                .queryParam(
                    "validatePrimaryKeyUniqueness",
                    String.valueOf(VALIDATE_PRIMARY_KEY_UNIQUENESS)))
        .andExpect(status().isForbidden());

    verifyAuthorizationCall(iamAction);
  }

  private static Stream<Arguments> testQuerySnapshotData() {
    return Stream.of(
        arguments(
            post(QUERY_SNAPSHOT_DATA_ENDPOINT, SNAPSHOT_ID, TABLE_NAME)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    TestUtils.mapToJson(
                        new QueryDataRequestModel()
                            .direction(DIRECTION)
                            .limit(LIMIT)
                            .offset(OFFSET)
                            .sort(COLUMN_NAME)
                            .filter(FILTER))),
            arguments(
                get(QUERY_SNAPSHOT_DATA_ENDPOINT, SNAPSHOT_ID, TABLE_NAME)
                    .queryParam("direction", String.valueOf(DIRECTION))
                    .queryParam("limit", String.valueOf(LIMIT))
                    .queryParam("offset", String.valueOf(OFFSET))
                    .queryParam("sort", COLUMN_NAME)
                    .queryParam("filter", FILTER))));
  }

  @ParameterizedTest
  @MethodSource
  void testQuerySnapshotData(MockHttpServletRequestBuilder request) throws Exception {
    mockValidators();
    var expectedSnapshotPreview =
        new SnapshotPreviewModel().addResultItem("row1").addResultItem("row2");
    when(snapshotService.retrievePreview(
            TEST_USER,
            SNAPSHOT_ID,
            TABLE_NAME,
            LIMIT,
            OFFSET,
            PDAO_ROW_ID_COLUMN,
            SqlSortDirection.from(DIRECTION),
            FILTER))
        .thenReturn(expectedSnapshotPreview);

    String actualJson = mvc.perform(request).andReturn().getResponse().getContentAsString();
    SnapshotPreviewModel actual = TestUtils.mapFromJson(actualJson, SnapshotPreviewModel.class);
    assertThat("Job model is returned", actual, equalTo(expectedSnapshotPreview));

    // Actual auth check is verified in SnapshotServiceTest.verifySnapshotReadableBySam()
    verify(snapshotService).verifySnapshotReadable(SNAPSHOT_ID, TEST_USER);
    verify(snapshotService)
        .retrievePreview(
            TEST_USER,
            SNAPSHOT_ID,
            TABLE_NAME,
            LIMIT,
            OFFSET,
            PDAO_ROW_ID_COLUMN,
            SqlSortDirection.from(DIRECTION),
            FILTER);
  }

  @Test
  void lockSnapshot() throws Exception {
    var lockId = "lockId";
    var resourceLocks = new ResourceLocks().exclusive(lockId);
    when(snapshotService.manualExclusiveLock(TEST_USER, SNAPSHOT_ID)).thenReturn(resourceLocks);
    mockValidators();

    var response =
        mvc.perform(put(LOCK_SNAPSHOT_ENDPOINT, SNAPSHOT_ID))
            .andExpect(status().is2xxSuccessful())
            .andReturn()
            .getResponse()
            .getContentAsString();
    ResourceLocks resultingLocks = TestUtils.mapFromJson(response, ResourceLocks.class);
    assertThat("ResourceLock object returns as expected", resultingLocks, equalTo(resourceLocks));
    verifyAuthorizationCall(IamAction.LOCK_RESOURCE);
    verify(snapshotService).manualExclusiveLock(TEST_USER, SNAPSHOT_ID);
  }

  @Test
  void unlockSnapshot() throws Exception {
    var lockId = "lockId";
    var resourceLocks = new ResourceLocks();
    var unlockRequest = new UnlockResourceRequest().lockName(lockId);
    when(snapshotService.manualExclusiveUnlock(TEST_USER, SNAPSHOT_ID, unlockRequest))
        .thenReturn(resourceLocks);
    mockValidators();

    var response =
        mvc.perform(
                put(UNLOCK_SNAPSHOT_ENDPOINT, SNAPSHOT_ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(unlockRequest)))
            .andExpect(status().is2xxSuccessful())
            .andReturn()
            .getResponse()
            .getContentAsString();
    ResourceLocks resultingLocks = TestUtils.mapFromJson(response, ResourceLocks.class);
    assertThat("ResourceLock object returns as expected", resultingLocks, equalTo(resourceLocks));
    verifyAuthorizationCall(IamAction.UNLOCK_RESOURCE);
    verify(snapshotService).manualExclusiveUnlock(TEST_USER, SNAPSHOT_ID, unlockRequest);
  }

  @Test
  void createSnapshot() throws Exception {
    mockValidators();
    Dataset dataset = new Dataset().id(DATASET_ID);
    when(snapshotService.getSourceDatasetFromSnapshotRequest(SNAPSHOT_REQUEST_MODEL))
        .thenReturn(dataset);

    IamAction iamAction = IamAction.LINK_SNAPSHOT;

    when(snapshotService.createSnapshot(SNAPSHOT_REQUEST_MODEL, dataset, TEST_USER))
        .thenReturn(JOB_ID);
    when(jobService.retrieveJob(JOB_ID, TEST_USER)).thenReturn(JOB_MODEL);

    String actualJson =
        mvc.perform(
                post(SNAPSHOTS_ENDPOINT)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(SNAPSHOT_REQUEST_MODEL)))
            .andExpect(status().isAccepted())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JobModel actual = TestUtils.mapFromJson(actualJson, JobModel.class);
    verify(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASET, DATASET_ID.toString(), iamAction);
    assertThat("Job model is returned", actual, equalTo(JOB_MODEL));
  }

  @Test
  void createSnapshot_forbidden() throws Exception {
    mockValidators();
    Dataset dataset = new Dataset().id(DATASET_ID);
    when(snapshotService.getSourceDatasetFromSnapshotRequest(SNAPSHOT_REQUEST_MODEL))
        .thenReturn(dataset);

    IamAction iamAction = IamAction.LINK_SNAPSHOT;
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASET, DATASET_ID.toString(), iamAction);

    mvc.perform(
            post(SNAPSHOTS_ENDPOINT)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(SNAPSHOT_REQUEST_MODEL)))
        .andExpect(status().isForbidden());
  }

  @Test
  void retrieveSnapshot() throws Exception {
    var snapshotModel = new SnapshotModel().id(SNAPSHOT_ID);
    when(snapshotService.retrieveSnapshotModel(SNAPSHOT_ID, List.of(INCLUDE), TEST_USER))
        .thenReturn(snapshotModel);

    String actualJson =
        mvc.perform(
                get(SNAPSHOT_ID_ENDPOINT, SNAPSHOT_ID)
                    .queryParam("include", String.valueOf(INCLUDE)))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    SnapshotModel actual = TestUtils.mapFromJson(actualJson, SnapshotModel.class);
    assertThat("Snapshot model is returned", actual, equalTo(snapshotModel));

    verify(snapshotService).verifySnapshotReadable(SNAPSHOT_ID, TEST_USER);
  }

  @Test
  void retrieveSnapshot_forbidden() throws Exception {
    doThrow(ForbiddenException.class)
        .when(snapshotService)
        .verifySnapshotReadable(SNAPSHOT_ID, TEST_USER);

    mvc.perform(
            get(SNAPSHOT_ID_ENDPOINT, SNAPSHOT_ID).queryParam("include", String.valueOf(INCLUDE)))
        .andExpect(status().isForbidden());

    verify(snapshotService).verifySnapshotReadable(SNAPSHOT_ID, TEST_USER);
  }

  @Test
  void enumerateSnapshots() throws Exception {
    var expected =
        new EnumerateSnapshotModel()
            .total(1)
            .addItemsItem(new SnapshotSummaryModel().id(SNAPSHOT_ID))
            .addErrorsItem(new ErrorModel().message("unexpected error"));
    when(snapshotService.enumerateSnapshots(
            TEST_USER,
            OFFSET,
            LIMIT,
            SORT,
            SqlSortDirection.from(DIRECTION),
            FILTER,
            REGION,
            List.of(DATASET_ID),
            List.of(TAG),
            List.of(DUOS_ID)))
        .thenReturn(expected);

    String actualJson =
        mvc.perform(
                get(SNAPSHOTS_ENDPOINT)
                    .queryParam("offset", String.valueOf(OFFSET))
                    .queryParam("limit", String.valueOf(LIMIT))
                    .queryParam("sort", SORT.name())
                    .queryParam("direction", DIRECTION.name())
                    .queryParam("filter", FILTER)
                    .queryParam("region", REGION)
                    .queryParam("datasetIds", String.valueOf(DATASET_ID))
                    .queryParam("tags", TAG)
                    .queryParam("duosDatasetIds", DUOS_ID))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();

    EnumerateSnapshotModel actual = TestUtils.mapFromJson(actualJson, EnumerateSnapshotModel.class);
    assertThat("Snapshot enumeration model is returned", actual, equalTo(expected));
  }

  @Test
  void enumerateSnapshots_invalidParams() throws Exception {
    mvc.perform(get(SNAPSHOTS_ENDPOINT).param("limit", "-1")).andExpect(status().isBadRequest());
    verifyNoInteractions(snapshotService);
  }

  @Test
  void deleteSnapshot() throws Exception {
    when(snapshotService.deleteSnapshot(SNAPSHOT_ID, TEST_USER)).thenReturn(JOB_ID);
    when(jobService.retrieveJob(JOB_ID, TEST_USER)).thenReturn(JOB_MODEL);

    String actualJson =
        mvc.perform(delete(SNAPSHOT_ID_ENDPOINT, SNAPSHOT_ID))
            .andExpect(status().isAccepted())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JobModel actual = TestUtils.mapFromJson(actualJson, JobModel.class);
    assertThat("Job model is returned", actual, equalTo(JOB_MODEL));

    verifyAuthorizationCall(IamAction.DELETE);
  }

  @Test
  void deleteSnapshot_notFound() throws Exception {
    doThrow(SnapshotNotFoundException.class)
        .when(snapshotService)
        .retrieveSnapshotSummary(SNAPSHOT_ID);

    mvc.perform(delete(SNAPSHOT_ID_ENDPOINT, SNAPSHOT_ID)).andExpect(status().isNotFound());

    verifyNoInteractions(iamService);
  }

  @Test
  void deleteSnapshot_forbidden() throws Exception {
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID.toString(), IamAction.DELETE);

    mvc.perform(delete(SNAPSHOT_ID_ENDPOINT, SNAPSHOT_ID)).andExpect(status().isForbidden());

    verifyAuthorizationCall(IamAction.DELETE);
  }

  @Test
  void getSnapshotSnapshotBuilderSettings() throws Exception {
    mockValidators();
    mvc.perform(get(SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, SNAPSHOT_ID)).andExpect(status().isOk());
    verify(snapshotService).getSnapshotBuilderSettings(SNAPSHOT_ID);
    verifyAuthorizationCall(IamAction.GET_SNAPSHOT_BUILDER_SETTINGS);
  }

  @Test
  void getSnapshotSnapshotBuilderSettingsForbidden() throws Exception {
    mockValidators();
    IamAction iamAction = IamAction.GET_SNAPSHOT_BUILDER_SETTINGS;
    failValidation(iamAction);

    mvc.perform(get(SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, SNAPSHOT_ID))
        .andExpect(status().isForbidden());

    verifyAuthorizationCall(iamAction);
  }

  @Test
  void getSnapshotSnapshotBuilderSettingsNotFound() throws Exception {
    mockValidators();
    doThrow(NotFoundException.class).when(snapshotService).getSnapshotBuilderSettings(SNAPSHOT_ID);
    mvc.perform(get(SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, SNAPSHOT_ID))
        .andExpect(status().isNotFound());
  }

  @Test
  void updateSnapshotSnapshotBuilderSettings() throws Exception {
    mockValidators();
    var snapshotBuilderSettings = SnapshotBuilderTestData.SETTINGS;
    mvc.perform(
            put(SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, SNAPSHOT_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(snapshotBuilderSettings)))
        .andExpect(status().isOk());
    verify(snapshotService).updateSnapshotBuilderSettings(SNAPSHOT_ID, snapshotBuilderSettings);
    verifyAuthorizationCall(IamAction.UPDATE_SNAPSHOT);
  }

  @Test
  void updateSnapshotSnapshotBuilderSettingsForbidden() throws Exception {
    mockValidators();
    IamAction iamAction = IamAction.UPDATE_SNAPSHOT;
    failValidation(iamAction);

    mvc.perform(
            put(SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, SNAPSHOT_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(SnapshotBuilderTestData.SETTINGS)))
        .andExpect(status().isForbidden());

    verifyAuthorizationCall(iamAction);
  }

  @Test
  void deleteSnapshotBuilderSettings() throws Exception {
    mockValidators();
    mvc.perform(delete(SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, SNAPSHOT_ID)).andExpect(status().isOk());
    verify(snapshotService).deleteSnapshotBuilderSettings(SNAPSHOT_ID);
    verifyAuthorizationCall(IamAction.UPDATE_SNAPSHOT);
  }

  @Test
  void deleteSnapshotBuilderSettingsForbidden() throws Exception {
    mockValidators();
    IamAction iamAction = IamAction.UPDATE_SNAPSHOT;
    failValidation(iamAction);

    mvc.perform(delete(SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, SNAPSHOT_ID))
        .andExpect(status().isForbidden());

    verifyAuthorizationCall(iamAction);
  }

  @Test
  void testGetConceptChildren() throws Exception {
    SnapshotBuilderConceptsResponse expected = makeGetConceptChildrenResponse();
    when(snapshotBuilderService.getConceptChildren(SNAPSHOT_ID, CONCEPT_ID, TEST_USER))
        .thenReturn(expected);
    String actualJson =
        mvc.perform(get(GET_CONCEPT_CHILDREN_ENDPOINT, SNAPSHOT_ID, CONCEPT_ID))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    SnapshotBuilderConceptsResponse actual =
        TestUtils.mapFromJson(actualJson, SnapshotBuilderConceptsResponse.class);
    assertThat("Concept list and sql is returned", actual, equalTo(expected));

    verifyAuthorizationCall(IamAction.READ_AGGREGATE_DATA);
  }

  @Test
  void getSnapshotBuilderCount() throws Exception {
    mockValidators();
    var cohorts = List.of(SnapshotBuilderTestData.createCohort());
    int count = 1234;
    when(snapshotBuilderService.getCountResponse(SNAPSHOT_ID, cohorts, TEST_USER))
        .thenReturn(
            new SnapshotBuilderCountResponse()
                .result(new SnapshotBuilderCountResponseResult().total(count)));
    mvc.perform(
            post(GET_COUNT_ENDPOINT, SNAPSHOT_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(new SnapshotBuilderCountRequest().cohorts(cohorts))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result.total").value(count));
    verifyAuthorizationCall(IamAction.READ_AGGREGATE_DATA);
  }

  private static Stream<String> testEnumerateConcepts() {
    return Stream.of("cancer", "", null);
  }

  @ParameterizedTest
  @MethodSource
  void testEnumerateConcepts(String filterText) throws Exception {
    SnapshotBuilderConceptsResponse expected = makeGetConceptChildrenResponse();

    var domainId = 1234;

    when(snapshotBuilderService.enumerateConcepts(SNAPSHOT_ID, domainId, filterText, TEST_USER))
        .thenReturn(expected);
    String actualJson =
        mvc.perform(
                get(ENUMERATE_CONCEPTS_ENDPOINT, SNAPSHOT_ID)
                    .queryParam("domainId", String.valueOf(domainId))
                    .queryParam("filterText", filterText))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    SnapshotBuilderConceptsResponse actual =
        TestUtils.mapFromJson(actualJson, SnapshotBuilderConceptsResponse.class);
    assertThat("Concept list and sql is returned", actual, equalTo(expected));

    verifyAuthorizationCall(IamAction.READ_AGGREGATE_DATA);
  }

  @Test
  void getConceptHierarchy() throws Exception {
    var expected =
        new SnapshotBuilderGetConceptHierarchyResponse()
            .result(
                List.of(
                    new SnapshotBuilderParentConcept()
                        .parentId(1234)
                        .addChildrenItem(new SnapshotBuilderConcept().name("test"))));
    var conceptId = 1234;

    when(snapshotBuilderService.getConceptHierarchy(SNAPSHOT_ID, conceptId, TEST_USER))
        .thenReturn(expected);
    String actualJson =
        mvc.perform(get(GET_CONCEPT_HIERARCHY_ENDPOINT, SNAPSHOT_ID, conceptId))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    var actual =
        TestUtils.mapFromJson(actualJson, SnapshotBuilderGetConceptHierarchyResponse.class);
    assertThat(actual, equalTo(expected));
    verifyAuthorizationCall(IamAction.READ_AGGREGATE_DATA);
  }

  private SnapshotBuilderConceptsResponse makeGetConceptChildrenResponse() {
    return new SnapshotBuilderConceptsResponse()
        .sql("SELECT * FROM dataset")
        .result(
            List.of(
                new SnapshotBuilderConcept()
                    .count(100)
                    .name("Stub concept")
                    .hasChildren(true)
                    .id(CONCEPT_ID + 1)));
  }

  private void failValidation(IamAction iamAction) {
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID.toString(), iamAction);
  }

  /** Verify that snapshot authorization was checked. */
  private void verifyAuthorizationCall(IamAction iamAction) {
    verify(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID.toString(), iamAction);
  }

  private void mockValidators() {
    when(snapshotRequestValidator.supports(any())).thenReturn(true);
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(assetModelValidator.supports(any())).thenReturn(true);
  }
}
