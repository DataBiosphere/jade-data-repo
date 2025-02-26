package bio.terra.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(classes = {AdminApiController.class, GlobalExceptionHandler.class})
@Tag(Unit.TAG)
@WebMvcTest
class AdminApiControllerTest {

  @Autowired private MockMvc mvc;

  @MockitoBean private JobService jobService;
  @MockitoBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockitoBean private DrsService drsService;
  @MockitoBean private IamService iamService;
  @MockitoBean private DatasetService datasetService;
  @MockitoBean private SnapshotService snapshotService;
  @MockitoBean private ApplicationConfiguration applicationConfiguration;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID MODEL_ID = UUID.randomUUID();
  private static final String ADMIN_DATASETS_ENDPOINT = "/api/admin/v1/datasets/{id}";
  private static final String ADMIN_SNAPSHOTS_ENDPOINT = "/api/admin/v1/snapshots/{id}";
  private static final String ADMIN_INHERIT_STEWARD_ENDPOINT =
      "/api/admin/v1/datasets/{id}/inheritSteward";
  private static final IamForbiddenException FORBIDDEN_EXCEPTION =
      new IamForbiddenException("Forbidden");

  @BeforeEach
  void setUp() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void testAdminRetrieveDataset() throws Exception {
    when(iamService.isResourceTypeAdminAuthorized(any(), any(), any())).thenReturn(true);
    when(datasetService.retrieveDatasetModel(any(), any(), any()))
        .thenReturn(new DatasetModel().id(MODEL_ID));
    String json =
        mvc.perform(get(ADMIN_DATASETS_ENDPOINT, MODEL_ID))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    DatasetModel model = TestUtils.mapFromJson(json, DatasetModel.class);
    assertThat("Dataset Model is returned", model, notNullValue());
    assertThat("Dataset Model ID is correct", model.getId(), equalTo(MODEL_ID));
  }

  @Test
  void testAdminRetrieveDatasetInvalidId() throws Exception {
    mvc.perform(get(ADMIN_DATASETS_ENDPOINT, "not a UUID")).andExpect(status().isBadRequest());
  }

  @Test
  void testAdminRetrieveDatasetNotAuthorized() throws Exception {
    doThrow(FORBIDDEN_EXCEPTION)
        .when(iamService)
        .verifyResourceTypeAdminAuthorized(any(), any(), any());
    int status =
        mvc.perform(get(ADMIN_DATASETS_ENDPOINT, MODEL_ID)).andReturn().getResponse().getStatus();
    assertThat(status, equalTo(FORBIDDEN_EXCEPTION.getStatusCode().value()));
  }

  @Test
  void testAdminRetrieveDatasetNotFound() throws Exception {
    when(iamService.isResourceTypeAdminAuthorized(any(), any(), any())).thenReturn(true);
    doThrow(new DatasetNotFoundException("Dataset not found for id: " + MODEL_ID))
        .when(datasetService)
        .retrieveDatasetModel(any(), any(), any());
    mvc.perform(get(ADMIN_DATASETS_ENDPOINT, MODEL_ID)).andExpect(status().isNotFound());
  }

  @Test
  void testAdminRetrieveSnapshot() throws Exception {
    when(iamService.isResourceTypeAdminAuthorized(any(), any(), any())).thenReturn(true);
    when(snapshotService.retrieveSnapshotModel(any(), any(), any()))
        .thenReturn(new SnapshotModel().id(MODEL_ID));
    String json =
        mvc.perform(get(ADMIN_SNAPSHOTS_ENDPOINT, MODEL_ID))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    SnapshotModel model = TestUtils.mapFromJson(json, SnapshotModel.class);
    assertThat("Snapshot Model is returned", model, notNullValue());
    assertThat("Snapshot Model ID is correct", model.getId(), equalTo(MODEL_ID));
  }

  @Test
  void testAdminRetrieveSnapshotNotAuthorized() throws Exception {
    doThrow(FORBIDDEN_EXCEPTION)
        .when(iamService)
        .verifyResourceTypeAdminAuthorized(any(), any(), any());
    int status =
        mvc.perform(get(ADMIN_SNAPSHOTS_ENDPOINT, MODEL_ID)).andReturn().getResponse().getStatus();
    assertThat(status, equalTo(FORBIDDEN_EXCEPTION.getStatusCode().value()));
  }

  @Test
  void testAdminRetrieveSnapshotInvalidId() throws Exception {
    mvc.perform(get(ADMIN_SNAPSHOTS_ENDPOINT, "not a UUID")).andExpect(status().isBadRequest());
  }

  @Test
  void testAdminRetrieveSnapshotNotFound() throws Exception {
    when(iamService.isResourceTypeAdminAuthorized(any(), any(), any())).thenReturn(true);
    doThrow(new SnapshotNotFoundException("Snapshot not found - id: " + MODEL_ID))
        .when(snapshotService)
        .retrieveSnapshotModel(any(), any(), any());
    mvc.perform(get(ADMIN_SNAPSHOTS_ENDPOINT, MODEL_ID)).andExpect(status().isNotFound());
  }

  @Test
  void testAdminInheritStewardEnable() throws Exception {
    String jobId = "jobId";
    when(iamService.isResourceTypeAdminAuthorized(
            any(), eq(IamResourceType.DATASET), eq(IamAction.ADMIN_TOGGLE_INHERIT_STEWARD)))
        .thenReturn(true);
    when(datasetService.retrieveDatasetSummary(MODEL_ID))
        .thenReturn(new DatasetSummaryModel().id(MODEL_ID).inheritSteward(false));
    when(datasetService.enableInheritSteward(eq(MODEL_ID), any())).thenReturn(jobId);
    when(jobService.retrieveJob(eq(jobId), any()))
        .thenReturn(new JobModel().id(jobId).jobStatus(JobModel.JobStatusEnum.RUNNING));

    Boolean inheritSteward = true;
    String json =
        mvc.perform(
                put(ADMIN_INHERIT_STEWARD_ENDPOINT, MODEL_ID)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(inheritSteward)))
            .andExpect(status().is(202))
            .andReturn()
            .getResponse()
            .getContentAsString();
    JobModel model = TestUtils.mapFromJson(json, JobModel.class);
    assertThat("Job ID is returned", model.getId(), equalTo(jobId));
  }

  @Test
  void testAdminInheritStewardDisable() throws Exception {
    when(iamService.isResourceTypeAdminAuthorized(
            any(), eq(IamResourceType.DATASET), eq(IamAction.ADMIN_TOGGLE_INHERIT_STEWARD)))
        .thenReturn(true);
    when(datasetService.retrieveDatasetSummary(MODEL_ID))
        .thenReturn(new DatasetSummaryModel().id(MODEL_ID).inheritSteward(true));
    Boolean inheritSteward = false;
    mvc.perform(
            put(ADMIN_INHERIT_STEWARD_ENDPOINT, MODEL_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(inheritSteward)))
        .andExpect(status().isInternalServerError());
  }

  @Test
  void testAdminInheritStewardNotAuthorized() throws Exception {
    doThrow(FORBIDDEN_EXCEPTION)
        .when(iamService)
        .verifyResourceTypeAdminAuthorized(
            any(), eq(IamResourceType.DATASET), eq(IamAction.ADMIN_TOGGLE_INHERIT_STEWARD));
    Boolean inheritSteward = true;
    mvc.perform(
            put(ADMIN_INHERIT_STEWARD_ENDPOINT, MODEL_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(inheritSteward)))
        .andExpect(status().isForbidden());
  }

  @Test
  void testAdminInheritStewardInvalidId() throws Exception {
    Boolean inheritSteward = true;
    mvc.perform(
            put(ADMIN_INHERIT_STEWARD_ENDPOINT, "not a UUID")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(inheritSteward)))
        .andExpect(status().isBadRequest());
  }

  @Test
  void testAdminInheritStewardDatasetNotFound() throws Exception {
    when(iamService.isResourceTypeAdminAuthorized(
            any(), eq(IamResourceType.DATASET), eq(IamAction.ADMIN_TOGGLE_INHERIT_STEWARD)))
        .thenReturn(true);
    doThrow(new DatasetNotFoundException("Dataset not found for id: " + MODEL_ID))
        .when(datasetService)
        .retrieveDatasetSummary(MODEL_ID);
    Boolean inheritSteward = true;
    mvc.perform(
            put(ADMIN_INHERIT_STEWARD_ENDPOINT, MODEL_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(inheritSteward)))
        .andExpect(status().isNotFound());
  }

  @Test
  void testAdminInheritStewardDatasetAlreadySet() throws Exception {
    when(iamService.isResourceTypeAdminAuthorized(
            any(), eq(IamResourceType.DATASET), eq(IamAction.ADMIN_TOGGLE_INHERIT_STEWARD)))
        .thenReturn(true);
    when(datasetService.retrieveDatasetSummary(MODEL_ID))
        .thenReturn(new DatasetSummaryModel().id(MODEL_ID).inheritSteward(true));
    Boolean inheritSteward = true;
    mvc.perform(
            put(ADMIN_INHERIT_STEWARD_ENDPOINT, MODEL_ID)
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(inheritSteward)))
        .andExpect(status().isBadRequest());
  }
}
