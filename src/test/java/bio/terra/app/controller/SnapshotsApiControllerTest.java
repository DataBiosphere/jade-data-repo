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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.JobModel;
import bio.terra.model.QueryDataRequestModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
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

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final String JOB_ID = "a-job-id";
  private static final Boolean EXPORT_GCS_PATHS = false;
  private static final Boolean VALIDATE_PRIMARY_KEY_UNIQUENESS = true;
  private static final Boolean SIGN_URLS = true;
  private static final String TABLE_NAME = "table1";
  private static final String COLUMN_NAME = PDAO_ROW_ID_COLUMN;
  private static final int LIMIT = 100;
  private static final int OFFSET = 0;
  private static final SqlSortDirection DIRECTION = SqlSortDirection.ASC;
  private static final String FILTER = "";

  private static final String RETRIEVE_SNAPSHOT_ENDPOINT = "/api/repository/v1/snapshots/{id}";

  private static final String QUERY_SNAPSHOT_DATA_ENDPOINT =
      RETRIEVE_SNAPSHOT_ENDPOINT + "/data/{table}";
  private static final String EXPORT_SNAPSHOT_ENDPOINT = RETRIEVE_SNAPSHOT_ENDPOINT + "/export";

  @BeforeEach
  void setUp() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void testExportSnapshot() throws Exception {
    JobModel expected = new JobModel().id(JOB_ID).jobStatus(JobModel.JobStatusEnum.RUNNING);

    when(snapshotService.exportSnapshot(
            SNAPSHOT_ID, TEST_USER, EXPORT_GCS_PATHS, VALIDATE_PRIMARY_KEY_UNIQUENESS, SIGN_URLS))
        .thenReturn(JOB_ID);
    when(jobService.retrieveJob(JOB_ID, TEST_USER)).thenReturn(expected);

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
    assertThat("Job model is returned", actual, equalTo(expected));

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
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID.toString(), iamAction);

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
            DIRECTION,
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
            DIRECTION,
            FILTER);
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
