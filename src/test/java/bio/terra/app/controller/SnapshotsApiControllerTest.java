package bio.terra.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.JobModel;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

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

  private static final String RETRIEVE_SNAPSHOT_ENDPOINT = "/api/repository/v1/snapshots/{id}";
  private static final String EXPORT_SNAPSHOT_ENDPOINT = RETRIEVE_SNAPSHOT_ENDPOINT + "/export";

  @BeforeEach
  void setUp() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void testExportSnapshot() throws Exception {
    JobModel expected = new JobModel().id(JOB_ID).jobStatus(JobModel.JobStatusEnum.RUNNING);

    when(snapshotService.exportSnapshot(
            SNAPSHOT_ID, TEST_USER, EXPORT_GCS_PATHS, VALIDATE_PRIMARY_KEY_UNIQUENESS))
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
        .exportSnapshot(SNAPSHOT_ID, TEST_USER, EXPORT_GCS_PATHS, VALIDATE_PRIMARY_KEY_UNIQUENESS);
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

  /** Verify that snapshot authorization was checked. */
  private void verifyAuthorizationCall(IamAction iamAction) {
    verify(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID.toString(), iamAction);
  }
}
