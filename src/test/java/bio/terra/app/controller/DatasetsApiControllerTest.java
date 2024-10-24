package bio.terra.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.SqlSortDirection;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnStatisticsTextModel;
import bio.terra.model.ColumnStatisticsTextValue;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.QueryColumnStatisticsRequestModel;
import bio.terra.model.QueryDataRequestModel;
import bio.terra.model.ResourceLocks;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SqlSortDirectionAscDefault;
import bio.terra.model.UnlockResourceRequest;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.DataDeletionRequestValidator;
import bio.terra.service.dataset.DatasetRequestValidator;
import bio.terra.service.dataset.DatasetSchemaUpdateValidator;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.dataset.exception.DatasetDataException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import java.util.List;
import java.util.Set;
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
@ContextConfiguration(classes = {DatasetsApiController.class, GlobalExceptionHandler.class})
@Tag(Unit.TAG)
@WebMvcTest
class DatasetsApiControllerTest {
  @Autowired private MockMvc mvc;
  @MockBean private JobService jobService;
  @MockBean private DatasetRequestValidator datasetRequestValidator;
  @MockBean private DatasetService datasetService;
  @MockBean private IamService iamService;
  @MockBean private FileService fileService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private AssetModelValidator assetModelValidator;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private DataDeletionRequestValidator dataDeletionRequestValidator;
  @MockBean private DatasetSchemaUpdateValidator datasetSchemaUpdateValidator;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String DATASET_ID_ENDPOINT = "/api/repository/v1/datasets/{id}";
  private static final String LOCK_DATASET_ENDPOINT = DATASET_ID_ENDPOINT + "/lock";
  private static final String UNLOCK_DATASET_ENDPOINT = DATASET_ID_ENDPOINT + "/unlock";
  private static final String DATASET_INGEST_ENDPOINT = DATASET_ID_ENDPOINT + "/ingest";
  private static final DatasetRequestAccessIncludeModel INCLUDE =
      DatasetRequestAccessIncludeModel.NONE;
  private static final String QUERY_DATA_ENDPOINT = DATASET_ID_ENDPOINT + "/data/{table}";

  private static final String QUERY_COLUMN_STATISTICS_ENDPOINT =
      QUERY_DATA_ENDPOINT + "/statistics/{column}";

  private static final SqlSortDirectionAscDefault DIRECTION = SqlSortDirectionAscDefault.ASC;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final DatasetPatchRequestModel DATASET_PATCH_REQUEST =
      new DatasetPatchRequestModel().phsId("a-phs-id").description("a-description");
  private static final Set<IamAction> DATASET_PATCH_ACTIONS =
      Set.of(IamAction.MANAGE_SCHEMA, IamAction.UPDATE_PASSPORT_IDENTIFIER);
  private static final int LIMIT = 10;
  private static final int OFFSET = 0;
  private static final String FILTER = null;
  private static final String TABLE_NAME = "good_table";
  private IngestRequestModel ingestRequestModel;

  @BeforeEach
  void setUp() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
    ingestRequestModel =
        new IngestRequestModel()
            .table(TABLE_NAME)
            .format(IngestRequestModel.FormatEnum.JSON)
            .path("/path/to/controlfile.json");
  }

  @Test
  void testRetrieveDataset() throws Exception {
    DatasetModel expected = new DatasetModel().id(DATASET_ID);
    when(datasetService.retrieveDatasetModel(DATASET_ID, TEST_USER, List.of(INCLUDE)))
        .thenReturn(expected);

    String actualJson =
        mvc.perform(
                get(DATASET_ID_ENDPOINT, DATASET_ID).queryParam("include", String.valueOf(INCLUDE)))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    DatasetModel actual = TestUtils.mapFromJson(actualJson, DatasetModel.class);
    assertThat("Dataset model is returned", actual, equalTo(expected));

    verifyAuthorizationsCall(List.of(IamAction.READ_DATASET));
    verify(datasetService).retrieveDatasetModel(DATASET_ID, TEST_USER, List.of(INCLUDE));
  }

  @Test
  void testRetrieveDatasetNotFound() throws Exception {
    mockNotFound();
    mvc.perform(get(DATASET_ID_ENDPOINT, DATASET_ID).queryParam("include", String.valueOf(INCLUDE)))
        .andExpect(status().isNotFound());
    verifyNoInteractions(iamService);
  }

  @Test
  void testRetrieveDatasetForbidden() throws Exception {
    IamAction iamAction = IamAction.READ_DATASET;
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorizations(
            TEST_USER, IamResourceType.DATASET, DATASET_ID.toString(), List.of(iamAction));

    mvc.perform(get(DATASET_ID_ENDPOINT, DATASET_ID).queryParam("include", String.valueOf(INCLUDE)))
        .andExpect(status().isForbidden());

    verifyAuthorizationsCall(List.of(iamAction));
  }

  private void mockPatchDatasetIamActions() {
    when(datasetService.patchDatasetIamActions(DATASET_PATCH_REQUEST))
        .thenReturn(DATASET_PATCH_ACTIONS);
  }

  @Test
  void patchDataset() throws Exception {
    mockValidators();
    mockPatchDatasetIamActions();

    var patchedDatasetSummary =
        new DatasetSummaryModel().id(DATASET_ID).description("patched dataset");
    when(datasetService.patch(DATASET_ID, DATASET_PATCH_REQUEST, TEST_USER))
        .thenReturn(patchedDatasetSummary);

    String actualJson =
        mvc.perform(
                patch(DATASET_ID_ENDPOINT, DATASET_ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(DATASET_PATCH_REQUEST)))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    DatasetSummaryModel actual = TestUtils.mapFromJson(actualJson, DatasetSummaryModel.class);
    assertThat("Dataset summary is returned", actual, equalTo(patchedDatasetSummary));

    verify(iamService)
        .verifyAuthorizations(
            TEST_USER, IamResourceType.DATASET, DATASET_ID.toString(), DATASET_PATCH_ACTIONS);
  }

  @Test
  void patchDataset_notFound() throws Exception {
    mockValidators();
    mockPatchDatasetIamActions();
    mockNotFound();

    mvc.perform(
            patch(DATASET_ID_ENDPOINT, DATASET_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(DATASET_PATCH_REQUEST)))
        .andExpect(status().isNotFound());

    verifyNoInteractions(iamService);
    verify(datasetService, never()).patch(DATASET_ID, DATASET_PATCH_REQUEST, TEST_USER);
  }

  @Test
  void patchDataset_forbidden() throws Exception {
    mockValidators();
    mockPatchDatasetIamActions();
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorizations(
            TEST_USER, IamResourceType.DATASET, DATASET_ID.toString(), DATASET_PATCH_ACTIONS);

    mvc.perform(
            patch(DATASET_ID_ENDPOINT, DATASET_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(DATASET_PATCH_REQUEST)))
        .andExpect(status().isForbidden());

    verify(datasetService, never()).patch(DATASET_ID, DATASET_PATCH_REQUEST, TEST_USER);
  }

  @ParameterizedTest
  @MethodSource
  void testQueryDatasetDataById(String column, MockHttpServletRequestBuilder request)
      throws Exception {
    when(datasetService.retrieveData(
            TEST_USER,
            DATASET_ID,
            TABLE_NAME,
            LIMIT,
            OFFSET,
            column,
            SqlSortDirection.from(DIRECTION),
            FILTER))
        .thenReturn(new DatasetDataModel().addResultItem("hello").addResultItem("world"));
    mockValidators();
    mvc.perform(request).andExpect(status().isOk()).andExpect(jsonPath("$.result").isArray());
    verifyAuthorizationCall(IamAction.READ_DATA);

    verify(datasetService)
        .retrieveData(
            TEST_USER,
            DATASET_ID,
            TABLE_NAME,
            LIMIT,
            OFFSET,
            column,
            SqlSortDirection.from(DIRECTION),
            FILTER);
  }

  private static Stream<Arguments> testQueryDatasetDataById() {
    return Stream.of(
        arguments("goodColumn", postQueryDataRequest(TABLE_NAME, "goodColumn")),
        arguments("datarepo_row_id", getRequest(TABLE_NAME, "datarepo_row_id")));
  }

  private static Stream<Arguments> testQueryDatasetColumnStatistics() {
    return Stream.of(
        arguments(
            post(QUERY_COLUMN_STATISTICS_ENDPOINT, DATASET_ID, "good_table", "good_column")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    TestUtils.mapToJson(new QueryColumnStatisticsRequestModel().filter(FILTER)))),
        arguments(
            get(QUERY_COLUMN_STATISTICS_ENDPOINT, DATASET_ID, "good_table", "good_column")
                .queryParam("filter", FILTER)));
  }

  @ParameterizedTest
  @MethodSource
  void testQueryDatasetColumnStatistics(MockHttpServletRequestBuilder request) throws Exception {
    var table = "good_table";
    var column = "good_column";
    var expected =
        new ColumnStatisticsTextModel()
            .values(List.of(new ColumnStatisticsTextValue().value("hello").count(2)));

    when(datasetService.retrieveColumnStatistics(TEST_USER, DATASET_ID, table, column, FILTER))
        .thenReturn(expected);
    mockValidators();

    String result = mvc.perform(request).andReturn().getResponse().getContentAsString();
    ColumnStatisticsTextModel actual =
        TestUtils.mapFromJson(result, ColumnStatisticsTextModel.class);

    assertThat("Correct ColumnStatisticsTextModel is returned", actual, equalTo(expected));
    verifyAuthorizationCall(IamAction.READ_DATA);
    verify(datasetService).retrieveColumnStatistics(TEST_USER, DATASET_ID, table, column, FILTER);
  }

  private static Stream<Arguments> provideRequests() {
    return Stream.of(
        arguments(postQueryDataRequest(TABLE_NAME, "goodColumn")),
        arguments(getRequest(TABLE_NAME, "goodColumn")));
  }

  @ParameterizedTest
  @MethodSource("provideRequests")
  void testQueryDatasetDataNotFound(MockHttpServletRequestBuilder request) throws Exception {
    mockNotFound();
    mockValidators();
    mvc.perform(request).andExpect(status().isNotFound());
    verifyNoInteractions(iamService);
  }

  @ParameterizedTest
  @MethodSource("provideRequests")
  void testQueryDatasetDataForbidden(MockHttpServletRequestBuilder request) throws Exception {
    IamAction iamAction = IamAction.READ_DATA;
    mockForbidden(iamAction);
    mockValidators();
    mvc.perform(request).andExpect(status().isForbidden());

    verifyAuthorizationCall(iamAction);
  }

  @ParameterizedTest
  @MethodSource
  void testQueryDatasetDataRetrievalFails(MockHttpServletRequestBuilder request) throws Exception {
    var table = "bad_table";
    var column = "good_column";

    when(datasetService.retrieveData(
            TEST_USER,
            DATASET_ID,
            table,
            LIMIT,
            OFFSET,
            column,
            SqlSortDirection.from(DIRECTION),
            FILTER))
        .thenThrow(DatasetDataException.class);
    mockValidators();
    mvc.perform(request).andExpect(status().is5xxServerError());

    verifyAuthorizationCall(IamAction.READ_DATA);
    verify(datasetService)
        .retrieveData(
            TEST_USER,
            DATASET_ID,
            table,
            LIMIT,
            OFFSET,
            column,
            SqlSortDirection.from(DIRECTION),
            FILTER);
  }

  private static Stream<Arguments> testQueryDatasetDataRetrievalFails() {
    return Stream.of(
        arguments(postQueryDataRequest("bad_table", "good_column")),
        arguments(getRequest("bad_table", "good_column")));
  }

  static Stream<Arguments> testCreateCriteriaData() {
    return Stream.of(
        arguments(
            """
            {"kind":"domain","id":0}""",
            SnapshotBuilderCriteria.class,
            SnapshotBuilderCriteria.KindEnum.DOMAIN),
        arguments(
            """
            {"kind":"list","id":0,"values":[]}""",
            SnapshotBuilderProgramDataListCriteria.class,
            SnapshotBuilderCriteria.KindEnum.LIST),
        arguments(
            """
            {"kind":"range","id":0,"low":0,"high":10}""",
            SnapshotBuilderProgramDataRangeCriteria.class,
            SnapshotBuilderCriteria.KindEnum.RANGE));
  }

  @ParameterizedTest
  @MethodSource
  void testCreateCriteriaData(
      String json,
      Class<? extends SnapshotBuilderCriteria> criteriaClass,
      SnapshotBuilderCriteria.KindEnum kind)
      throws Exception {
    SnapshotBuilderCriteria criteria = TestUtils.mapFromJson(json, criteriaClass);
    assertThat(criteria.getKind(), equalTo(kind));
  }

  /** Mock so that the user does not hold `iamAction` on the dataset. */
  private void mockNotFound() {
    doThrow(DatasetNotFoundException.class).when(datasetService).retrieveDatasetSummary(DATASET_ID);
  }

  /** Mock so that the user does not hold `iamAction` on the dataset. */
  private void mockForbidden(IamAction iamAction) {
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASET, DATASET_ID.toString(), iamAction);
  }

  /** Verify that dataset authorization was checked. */
  private void verifyAuthorizationCall(IamAction iamAction) {
    verify(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASET, DATASET_ID.toString(), iamAction);
  }

  private void verifyAuthorizationsCall(List<IamAction> iamActions) {
    verify(iamService)
        .verifyAuthorizations(
            TEST_USER, IamResourceType.DATASET, DATASET_ID.toString(), iamActions);
  }

  private void mockValidators() {
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(datasetRequestValidator.supports(any())).thenReturn(true);
    when(assetModelValidator.supports(any())).thenReturn(true);
    when(dataDeletionRequestValidator.supports(any())).thenReturn(true);
    when(datasetSchemaUpdateValidator.supports(any())).thenReturn(true);
  }

  private static MockHttpServletRequestBuilder postQueryDataRequest(
      String tableName, String columnName) {
    return post(QUERY_DATA_ENDPOINT, DATASET_ID, tableName)
        .contentType(MediaType.APPLICATION_JSON)
        .content(
            TestUtils.mapToJson(
                new QueryDataRequestModel()
                    .direction(SqlSortDirectionAscDefault.ASC)
                    .limit(LIMIT)
                    .offset(OFFSET)
                    .sort(columnName)
                    .filter(FILTER)));
  }

  private static MockHttpServletRequestBuilder getRequest(String tableName, String columnName) {
    return get(QUERY_DATA_ENDPOINT, DATASET_ID, tableName)
        .queryParam("limit", String.valueOf(LIMIT))
        .queryParam("offset", String.valueOf(OFFSET))
        .queryParam("sort", columnName)
        .queryParam("direction", DIRECTION.name())
        .queryParam("filter", FILTER);
  }

  @Test
  void lockDataset() throws Exception {
    var resourceLocks = new ResourceLocks();
    when(datasetService.manualExclusiveLock(TEST_USER, DATASET_ID)).thenReturn(resourceLocks);
    mockValidators();

    var response =
        mvc.perform(put(LOCK_DATASET_ENDPOINT, DATASET_ID))
            .andExpect(status().is2xxSuccessful())
            .andReturn()
            .getResponse()
            .getContentAsString();
    ResourceLocks resultingLocks = TestUtils.mapFromJson(response, ResourceLocks.class);
    assertThat("ResourceLock object returns as expected", resultingLocks, equalTo(resourceLocks));

    verifyAuthorizationCall(IamAction.LOCK_RESOURCE);
    verify(datasetService).manualExclusiveLock(TEST_USER, DATASET_ID);
  }

  @Test
  void unlockDataset() throws Exception {
    var lockId = "lockId";
    var resourceLocks = new ResourceLocks().exclusive(lockId);
    var unlockRequest = new UnlockResourceRequest().lockName(lockId).forceUnlock(false);
    when(datasetService.manualUnlock(TEST_USER, DATASET_ID, unlockRequest))
        .thenReturn(resourceLocks);
    mockValidators();

    var response =
        mvc.perform(
                put(UNLOCK_DATASET_ENDPOINT, DATASET_ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(unlockRequest)))
            .andExpect(status().is2xxSuccessful())
            .andReturn()
            .getResponse()
            .getContentAsString();
    ResourceLocks resultingLocks = TestUtils.mapFromJson(response, ResourceLocks.class);
    assertThat("ResourceLock object returns as expected", resultingLocks, equalTo(resourceLocks));
    verifyAuthorizationCall(IamAction.UNLOCK_RESOURCE);
    verify(datasetService).manualUnlock(TEST_USER, DATASET_ID, unlockRequest);
  }

  @Test
  void ingestDataset_forbidden() throws Exception {
    IamAction iamAction = IamAction.INGEST_DATA;
    mockForbidden(iamAction);
    mockValidators();

    mvc.perform(
            post(DATASET_INGEST_ENDPOINT, DATASET_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(ingestRequestModel)))
        .andExpect(status().isForbidden());

    verifyAuthorizationCall(iamAction);
  }

  private static Stream<Arguments> ingestDataset_updateStrategy_supported() {
    return Stream.of(
        arguments(CloudPlatform.GCP, null),
        arguments(CloudPlatform.GCP, IngestRequestModel.UpdateStrategyEnum.APPEND),
        arguments(CloudPlatform.GCP, IngestRequestModel.UpdateStrategyEnum.REPLACE),
        arguments(CloudPlatform.GCP, IngestRequestModel.UpdateStrategyEnum.MERGE),
        arguments(CloudPlatform.AZURE, null),
        arguments(CloudPlatform.AZURE, IngestRequestModel.UpdateStrategyEnum.APPEND));
  }

  @ParameterizedTest
  @MethodSource
  void ingestDataset_updateStrategy_supported(
      CloudPlatform platform, IngestRequestModel.UpdateStrategyEnum updateStrategy)
      throws Exception {
    ingestRequestModel.updateStrategy(updateStrategy);
    String jobId = "a-job-id";
    JobModel expectedJob = new JobModel().id(jobId).jobStatus(JobModel.JobStatusEnum.RUNNING);

    mockValidators();
    when(datasetService.retrieveDatasetSummary(DATASET_ID))
        .thenReturn(new DatasetSummaryModel().cloudPlatform(platform));
    when(datasetService.ingestDataset(
            eq(DATASET_ID.toString()), any(IngestRequestModel.class), eq(TEST_USER)))
        .thenReturn(jobId);
    when(jobService.retrieveJob(jobId, TEST_USER)).thenReturn(expectedJob);

    String actualJson =
        mvc.perform(
                post(DATASET_INGEST_ENDPOINT, DATASET_ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(ingestRequestModel)))
            .andExpect(status().isAccepted())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JobModel job = TestUtils.mapFromJson(actualJson, JobModel.class);
    assertThat(job, equalTo(expectedJob));

    verifyAuthorizationCall(IamAction.INGEST_DATA);
  }

  private static Stream<Arguments> ingestDataset_updateStrategy_unsupported() {
    return Stream.of(
        arguments(CloudPlatform.AZURE, IngestRequestModel.UpdateStrategyEnum.REPLACE),
        arguments(CloudPlatform.AZURE, IngestRequestModel.UpdateStrategyEnum.MERGE));
  }

  @ParameterizedTest
  @MethodSource
  void ingestDataset_updateStrategy_unsupported(
      CloudPlatform platform, IngestRequestModel.UpdateStrategyEnum updateStrategy)
      throws Exception {
    mockValidators();
    when(datasetService.retrieveDatasetSummary(DATASET_ID))
        .thenReturn(new DatasetSummaryModel().cloudPlatform(platform));

    String actualJson =
        mvc.perform(
                post(DATASET_INGEST_ENDPOINT, DATASET_ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        TestUtils.mapToJson(ingestRequestModel.updateStrategy(updateStrategy))))
            .andExpect(status().isBadRequest())
            .andReturn()
            .getResponse()
            .getContentAsString();
    ErrorModel error = TestUtils.mapFromJson(actualJson, ErrorModel.class);
    assertThat(error.getMessage(), equalTo("Invalid ingest parameters detected"));
    String expectedErrorDetail =
        "Ingests to Azure datasets can only use 'append' as an update strategy, was '%s'."
            .formatted(updateStrategy);
    assertThat(error.getErrorDetail(), contains(expectedErrorDetail));

    verifyAuthorizationCall(IamAction.INGEST_DATA);
  }
}
