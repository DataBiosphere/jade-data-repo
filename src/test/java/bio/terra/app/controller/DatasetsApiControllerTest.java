package bio.terra.app.controller;

import static bio.terra.service.snapshotbuilder.SnapshotBuilderTestData.SETTINGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.ColumnStatisticsTextModel;
import bio.terra.model.ColumnStatisticsTextValue;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.model.LookupColumnStatisticsRequestModel;
import bio.terra.model.QueryDataRequestModel;
import bio.terra.model.SqlSortDirection;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(classes = {DatasetsApiController.class, GlobalExceptionHandler.class})
@Tag("bio.terra.common.category.Unit")
@WebMvcTest
public class DatasetsApiControllerTest {
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
  private static final DatasetRequestAccessIncludeModel INCLUDE =
      DatasetRequestAccessIncludeModel.NONE;
  private static final String QUERY_DATA_ENDPOINT = RETRIEVE_DATASET_ENDPOINT + "/data/{table}";

  private static final String QUERY_COLUMN_STATISTICS_ENDPOINT =
      QUERY_DATA_ENDPOINT + "/statistics/{column}";
  private static final String GET_SNAPSHOT_BUILDER_SETTINGS_ENDPOINT =
      RETRIEVE_DATASET_ENDPOINT + "/snapshotBuilder/settings";
  private static final String GET_CONCEPTS_ENDPOINT =
      RETRIEVE_DATASET_ENDPOINT + "/snapshotBuilder/concepts/{parentConcept}";
  private static final SqlSortDirection DIRECTION = SqlSortDirection.ASC;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final Integer CONCEPT_ID = 0;
  private static final int LIMIT = 10;
  private static final int OFFSET = 0;
  private static final String FILTER = null;

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
  @ValueSource(strings = {"good_column", "datarepo_row_id"})
  void deprecated_testDatasetLookupDataById(String column) throws Exception {
    var table = "good_table";
    when(datasetService.retrieveData(
            TEST_USER, DATASET_ID, table, LIMIT, OFFSET, column, DIRECTION, FILTER))
        .thenReturn(new DatasetDataModel().addResultItem("hello").addResultItem("world"));

    deprecated_performLookupData(table, column)
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result").isArray());
    verifyAuthorizationCall(IamAction.READ_DATA);

    verify(datasetService)
        .retrieveData(TEST_USER, DATASET_ID, table, LIMIT, OFFSET, column, DIRECTION, FILTER);
  }

  @ParameterizedTest
  @ValueSource(strings = {"good_column", "datarepo_row_id"})
  void testQueryDatasetDataById(String column) throws Exception {
    var table = "good_table";
    when(datasetService.retrieveData(
            TEST_USER, DATASET_ID, table, LIMIT, OFFSET, column, DIRECTION, FILTER))
        .thenReturn(new DatasetDataModel().addResultItem("hello").addResultItem("world"));

    performQueryData(table, column)
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result").isArray());
    verifyAuthorizationCall(IamAction.READ_DATA);

    verify(datasetService)
        .retrieveData(TEST_USER, DATASET_ID, table, LIMIT, OFFSET, column, DIRECTION, FILTER);
  }

  @Test
  void deprecated_testLookupDatasetColumnStatistics() throws Exception {
    var table = "good_table";
    var column = "good_column";
    when(datasetService.retrieveColumnStatistics(TEST_USER, DATASET_ID, table, column, FILTER))
        .thenReturn(
            new ColumnStatisticsTextModel()
                .values(List.of(new ColumnStatisticsTextValue().value("hello").count(2))));

    MvcResult result =
        mvc.perform(get(QUERY_COLUMN_STATISTICS_ENDPOINT, DATASET_ID, table, column))
            .andExpect(status().isOk())
            .andReturn();
    String content = result.getResponse().getContentAsString();
    assertThat("Content contains value", content, containsString("hello"));
    verifyAuthorizationCall(IamAction.READ_DATA);

    verify(datasetService).retrieveColumnStatistics(TEST_USER, DATASET_ID, table, column, FILTER);
  }

  @Test
  void testQueryDatasetColumnStatistics() throws Exception {
    var table = "good_table";
    var column = "good_column";
    when(datasetService.retrieveColumnStatistics(TEST_USER, DATASET_ID, table, column, FILTER))
        .thenReturn(
            new ColumnStatisticsTextModel()
                .values(List.of(new ColumnStatisticsTextValue().value("hello").count(2))));

    mockValidators();
    MvcResult result =
        mvc.perform(
                post(QUERY_COLUMN_STATISTICS_ENDPOINT, DATASET_ID, table, column)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        TestUtils.mapToJson(
                            new LookupColumnStatisticsRequestModel().filter(FILTER))))
            .andReturn();
    String content = result.getResponse().getContentAsString();
    assertThat("Content contains value", content, containsString("hello"));
    verifyAuthorizationCall(IamAction.READ_DATA);

    verify(datasetService).retrieveColumnStatistics(TEST_USER, DATASET_ID, table, column, FILTER);
  }

  @Test
  void deprecated_testLookUpDatasetDataNotFound() throws Exception {
    mockNotFound();
    deprecated_performLookupData("table", "column").andExpect(status().isNotFound());
    verifyNoInteractions(iamService);
  }

  @Test
  void testQueryDatasetDataNotFound() throws Exception {
    mockNotFound();
    performQueryData("table", "column").andExpect(status().isNotFound());
    verifyNoInteractions(iamService);
  }

  @Test
  void deprecated_testLookupDatasetDataForbidden() throws Exception {
    IamAction iamAction = IamAction.READ_DATA;
    mockForbidden(iamAction);
    deprecated_performLookupData("table", "column").andExpect(status().isForbidden());

    verifyAuthorizationCall(iamAction);
  }

  @Test
  void testQueryDatasetDataForbidden() throws Exception {
    IamAction iamAction = IamAction.READ_DATA;
    mockForbidden(iamAction);
    deprecated_performLookupData("table", "column").andExpect(status().isForbidden());

    verifyAuthorizationCall(iamAction);
  }

  @Test
  void deprecated_testDatasetViewDataRetrievalFails() throws Exception {
    var table = "bad_table";
    var column = "good_column";

    when(datasetService.retrieveData(
            TEST_USER, DATASET_ID, table, LIMIT, OFFSET, column, DIRECTION, FILTER))
        .thenThrow(DatasetDataException.class);
    deprecated_performLookupData(table, column).andExpect(status().is5xxServerError());

    verifyAuthorizationCall(IamAction.READ_DATA);
    verify(datasetService)
        .retrieveData(TEST_USER, DATASET_ID, table, LIMIT, OFFSET, column, DIRECTION, FILTER);
  }

  @Test
  void testQueryDatasetDataRetrievalFails() throws Exception {
    var table = "bad_table";
    var column = "good_column";

    when(datasetService.retrieveData(
            TEST_USER, DATASET_ID, table, LIMIT, OFFSET, column, DIRECTION, FILTER))
        .thenThrow(DatasetDataException.class);
    performQueryData(table, column).andExpect(status().is5xxServerError());

    verifyAuthorizationCall(IamAction.READ_DATA);
    verify(datasetService)
        .retrieveData(TEST_USER, DATASET_ID, table, LIMIT, OFFSET, column, DIRECTION, FILTER);
  }

  @Test
  void testUpdateSnapshotBuilderSettings() throws Exception {
    when(datasetService.retrieveDatasetModel(
            DATASET_ID,
            TEST_USER,
            List.of(DatasetRequestAccessIncludeModel.SNAPSHOT_BUILDER_SETTINGS)))
        .thenReturn(new DatasetModel());
    when(snapshotBuilderService.updateSnapshotBuilderSettings(DATASET_ID, SETTINGS))
        .thenReturn(SETTINGS);
    mockValidators();

    mvc.perform(
            post(GET_SNAPSHOT_BUILDER_SETTINGS_ENDPOINT, DATASET_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(SETTINGS)))
        .andExpect(status().is2xxSuccessful())
        .andReturn();

    verifyAuthorizationCall(IamAction.UPDATE_SNAPSHOT_BUILDER_SETTINGS);
    verify(snapshotBuilderService).updateSnapshotBuilderSettings(DATASET_ID, SETTINGS);
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
    verify(snapshotBuilderService).getConceptChildren(DATASET_ID, CONCEPT_ID);
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

  private ResultActions deprecated_performLookupData(String table, String column) throws Exception {
    return mvc.perform(
        get(QUERY_DATA_ENDPOINT, DATASET_ID, table)
            .queryParam("limit", String.valueOf(LIMIT))
            .queryParam("offset", String.valueOf(OFFSET))
            .queryParam("sort", column)
            .queryParam("direction", DIRECTION.name()));
  }

  private ResultActions performQueryData(String table, String column) throws Exception {
    mockValidators();
    return mvc.perform(
        post(QUERY_DATA_ENDPOINT, DATASET_ID, table)
            .contentType(MediaType.APPLICATION_JSON)
            .content(
                TestUtils.mapToJson(
                    new QueryDataRequestModel()
                        .direction(DIRECTION)
                        .limit(LIMIT)
                        .offset(OFFSET)
                        .sort(column)
                        .filter(FILTER))));
  }

  private void mockValidators() {
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(datasetRequestValidator.supports(any())).thenReturn(true);
    when(assetModelValidator.supports(any())).thenReturn(true);
    when(dataDeletionRequestValidator.supports(any())).thenReturn(true);
    when(datasetSchemaUpdateValidator.supports(any())).thenReturn(true);
  }
}
