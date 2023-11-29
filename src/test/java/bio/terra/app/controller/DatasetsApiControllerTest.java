package bio.terra.app.controller;

import static bio.terra.service.snapshotbuilder.SnapshotBuilderTestData.SETTINGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.SqlSortDirection;
import bio.terra.common.TestUtils;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.ColumnStatisticsTextModel;
import bio.terra.model.ColumnStatisticsTextValue;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.JobModel;
import bio.terra.model.QueryColumnStatisticsRequestModel;
import bio.terra.model.QueryDataRequestModel;
import bio.terra.model.SnapshotBuilderAccessRequest;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCountRequest;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDatasetConceptSet;
import bio.terra.model.SnapshotBuilderFeatureValueGroup;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderRequest;
import bio.terra.model.SqlSortDirectionAscDefault;
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
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
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
@ContextConfiguration(classes = {DatasetsApiController.class, GlobalExceptionHandler.class})
@Tag("bio.terra.common.category.Unit")
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
  @MockBean private SnapshotBuilderService snapshotBuilderService;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String RETRIEVE_DATASET_ENDPOINT = "/api/repository/v1/datasets/{id}";
  private static final String LOCK_DATASET_ENDPOINT = "/api/repository/v1/datasets/{id}/lock";
  private static final String UNLOCK_DATASET_ENDPOINT = "/api/repository/v1/datasets/{id}/unlock";
  private static final String REQUEST_SNAPSHOT_ENDPOINT =
      RETRIEVE_DATASET_ENDPOINT + "/createSnapshotRequest";
  private static final DatasetRequestAccessIncludeModel INCLUDE =
      DatasetRequestAccessIncludeModel.NONE;
  private static final String QUERY_DATA_ENDPOINT = RETRIEVE_DATASET_ENDPOINT + "/data/{table}";

  private static final String QUERY_COLUMN_STATISTICS_ENDPOINT =
      QUERY_DATA_ENDPOINT + "/statistics/{column}";
  private static final String GET_SNAPSHOT_BUILDER_SETTINGS_ENDPOINT =
      RETRIEVE_DATASET_ENDPOINT + "/snapshotBuilder/settings";
  private static final String GET_CONCEPTS_ENDPOINT =
      RETRIEVE_DATASET_ENDPOINT + "/snapshotBuilder/concepts/{parentConcept}";
  private static final String GET_COUNT_ENDPOINT =
      RETRIEVE_DATASET_ENDPOINT + "/snapshotBuilder/count";
  private static final SqlSortDirectionAscDefault DIRECTION = SqlSortDirectionAscDefault.ASC;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final Integer CONCEPT_ID = 0;
  private static final int LIMIT = 10;
  private static final int OFFSET = 0;
  private static final String FILTER = null;
  private static final String TABLE_NAME = "good_table";

  @BeforeEach
  void setUp() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void testRetrieveDataset() throws Exception {
    DatasetModel expected = new DatasetModel().id(DATASET_ID);
    when(datasetService.retrieveDatasetModel(DATASET_ID, TEST_USER, List.of(INCLUDE)))
        .thenReturn(expected);

    String actualJson =
        mvc.perform(
                get(RETRIEVE_DATASET_ENDPOINT, DATASET_ID)
                    .queryParam("include", String.valueOf(INCLUDE)))
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
    mvc.perform(
            get(RETRIEVE_DATASET_ENDPOINT, DATASET_ID)
                .queryParam("include", String.valueOf(INCLUDE)))
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

    mvc.perform(
            get(RETRIEVE_DATASET_ENDPOINT, DATASET_ID)
                .queryParam("include", String.valueOf(INCLUDE)))
        .andExpect(status().isForbidden());

    verifyAuthorizationsCall(List.of(iamAction));
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
        arguments("goodColumn", postRequset(TABLE_NAME, "goodColumn")),
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
        arguments(postRequset(TABLE_NAME, "goodColumn")),
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
        arguments(postRequset("bad_table", "good_column")),
        arguments(getRequest("bad_table", "good_column")));
  }

  @Test
  void testUpdateSnapshotBuilderSettings() throws Exception {
    when(datasetService.retrieveDatasetModel(
            DATASET_ID,
            TEST_USER,
            List.of(DatasetRequestAccessIncludeModel.SNAPSHOT_BUILDER_SETTINGS)))
        .thenReturn(new DatasetModel());

    mockValidators();

    mvc.perform(
            post(GET_SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, DATASET_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(SETTINGS)))
        .andExpect(status().is2xxSuccessful())
        .andReturn();

    verifyAuthorizationCall(IamAction.UPDATE_SNAPSHOT_BUILDER_SETTINGS);
    verify(datasetService).updateDatasetSnapshotBuilderSettings(DATASET_ID, SETTINGS);
  }

  static Stream<Arguments> testCreateCriteriaData() {
    return Stream.of(
        arguments(
            """
            {"kind":"domain","name":"name","id":0}""",
            SnapshotBuilderCriteria.class,
            SnapshotBuilderCriteria.KindEnum.DOMAIN),
        arguments(
            """
            {"kind":"list","name":"name","id":0,"values":[]}""",
            SnapshotBuilderProgramDataListCriteria.class,
            SnapshotBuilderCriteria.KindEnum.LIST),
        arguments(
            """
            {"kind":"range","name":"name","id":0,"low":0,"high":10}""",
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
    assertThat(criteria.getName(), equalTo("name"));
    assertThat(criteria.getKind(), equalTo(kind));
  }

  @Test
  void testCreateSnapshotRequest() throws Exception {
    mockValidators();
    SnapshotBuilderAccessRequest expected =
        new SnapshotBuilderAccessRequest()
            .name("name")
            .researchPurposeStatement("purpose")
            .datasetRequest(
                new SnapshotBuilderRequest()
                    .addCohortsItem(createTestCohort())
                    .addConceptSetsItem(
                        new SnapshotBuilderDatasetConceptSet()
                            .name("conceptSet")
                            .featureValueGroupName("featureValueGroupName"))
                    .addValueSetsItem(
                        new SnapshotBuilderFeatureValueGroup()
                            .name("valueGroup")
                            .addValuesItem("value")));
    when(snapshotBuilderService.createSnapshotRequest(DATASET_ID, expected)).thenReturn(expected);
    String actualJson =
        mvc.perform(
                post(REQUEST_SNAPSHOT_ENDPOINT, DATASET_ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(expected)))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    SnapshotBuilderAccessRequest actual =
        TestUtils.mapFromJson(actualJson, SnapshotBuilderAccessRequest.class);
    assertThat("The method returned the expected request", actual, equalTo(expected));
    verifyAuthorizationCall(IamAction.VIEW_SNAPSHOT_BUILDER_SETTINGS);
  }

  private static SnapshotBuilderCohort createTestCohort() {
    return new SnapshotBuilderCohort()
        .name("cohort")
        .addCriteriaGroupsItem(
            new SnapshotBuilderCriteriaGroup()
                .addCriteriaItem(
                    new SnapshotBuilderProgramDataListCriteria()
                        .kind(SnapshotBuilderCriteria.KindEnum.LIST))
                .addCriteriaItem(
                    new SnapshotBuilderCriteria().kind(SnapshotBuilderCriteria.KindEnum.DOMAIN))
                .addCriteriaItem(
                    new SnapshotBuilderProgramDataRangeCriteria()
                        .kind(SnapshotBuilderCriteria.KindEnum.RANGE)));
  }

  @Test
  void testGetConcepts() throws Exception {
    SnapshotBuilderGetConceptsResponse expected =
        new SnapshotBuilderGetConceptsResponse()
            .sql("SELECT * FROM dataset")
            .result(
                List.of(
                    new SnapshotBuilderConcept()
                        .count(100)
                        .name("Stub concept")
                        .hasChildren(true)
                        .id(CONCEPT_ID + 1)));
    when(snapshotBuilderService.getConceptChildren(DATASET_ID, CONCEPT_ID)).thenReturn(expected);
    String actualJson =
        mvc.perform(get(GET_CONCEPTS_ENDPOINT, DATASET_ID, CONCEPT_ID))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    SnapshotBuilderGetConceptsResponse actual =
        TestUtils.mapFromJson(actualJson, SnapshotBuilderGetConceptsResponse.class);
    assertThat("Concept list and sql is returned", actual, equalTo(expected));

    verifyAuthorizationCall(IamAction.VIEW_SNAPSHOT_BUILDER_SETTINGS);
  }

  @Test
  void getSnapshotBuilderCount() throws Exception {
    mockValidators();
    var cohorts = List.of(createTestCohort());
    int count = 1234;
    when(snapshotBuilderService.getCountResponse(DATASET_ID, cohorts))
        .thenReturn(
            new SnapshotBuilderCountResponse()
                .result(new SnapshotBuilderCountResponseResult().total(count)));
    mvc.perform(
            post(GET_COUNT_ENDPOINT, DATASET_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(new SnapshotBuilderCountRequest().cohorts(cohorts))))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result.total").value(count));
    verifyAuthorizationCall(IamAction.VIEW_SNAPSHOT_BUILDER_SETTINGS);
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

  private static MockHttpServletRequestBuilder postRequset(String tableName, String columnName) {
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
    var fakeFlightId = "fakeFlightId";
    when(datasetService.manualExclusiveLock(TEST_USER, DATASET_ID)).thenReturn(fakeFlightId);
    when(jobService.retrieveJob(fakeFlightId, TEST_USER))
        .thenReturn(new JobModel().id(fakeFlightId));
    mockValidators();

    mvc.perform(put(LOCK_DATASET_ENDPOINT, DATASET_ID))
        .andExpect(status().is2xxSuccessful())
        .andReturn();
    verifyAuthorizationCall(IamAction.MANAGE_SCHEMA);
    verify(datasetService).manualExclusiveLock(TEST_USER, DATASET_ID);
  }

  @Test
  void unlockDataset() throws Exception {
    var lockId = "lockId";
    var fakeFlightId = "fakeFlightId";
    when(datasetService.manualUnlock(TEST_USER, DATASET_ID, lockId)).thenReturn(fakeFlightId);
    when(jobService.retrieveJob(fakeFlightId, TEST_USER))
        .thenReturn(new JobModel().id(fakeFlightId));
    mockValidators();

    mvc.perform(put(UNLOCK_DATASET_ENDPOINT, DATASET_ID).queryParam("lockName", lockId))
        .andExpect(status().is2xxSuccessful())
        .andReturn();
    verifyAuthorizationCall(IamAction.MANAGE_SCHEMA);
    verify(datasetService).manualUnlock(TEST_USER, DATASET_ID, lockId);
  }
}
