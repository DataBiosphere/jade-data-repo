package bio.terra.app.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.usermetrics.BardClient;
import bio.terra.common.SqlSortDirection;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.DatasetRequestValidator;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.job.JobService;
import bio.terra.service.job.JobService.JobResultWithStatus;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@ContextConfiguration(classes = {JobsApiController.class, GlobalExceptionHandler.class})
@WebMvcTest
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
class JobsApiControllerTest {
  private static final String FLIGHT_CLASS = "bio.terra.app.flight.TestFlight";
  private static final JobModel JOB_1 =
      new JobModel().id("foo").jobStatus(JobStatusEnum.SUCCEEDED).className(FLIGHT_CLASS);
  private static final JobModel JOB_2 =
      new JobModel().id("bar").jobStatus(JobStatusEnum.FAILED).className(FLIGHT_CLASS);

  private static final String ENUMERATE_JOBS_ENDPOINT = "/api/repository/v1/jobs";
  private static final String RETRIEVE_JOB_ENDPOINT = "/api/repository/v1/jobs/{id}";
  private static final String RETRIEVE_JOB_RESULT_ENDPOINT = "/api/repository/v1/jobs/{id}/result";

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @Autowired private MockMvc mvc;

  @MockBean private JobService jobService;

  @MockBean private BardClient bardClient;
  @MockBean private DatasetRequestValidator datasetRequestValidator;
  @MockBean private SnapshotRequestValidator snapshotRequestValidator;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private PolicyMemberValidator policyMemberValidator;
  @MockBean private AssetModelValidator assetModelValidator;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;

  @BeforeEach
  void beforeEach() {
    when(datasetRequestValidator.supports(any())).thenReturn(true);
    when(snapshotRequestValidator.supports(any())).thenReturn(true);
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(policyMemberValidator.supports(any())).thenReturn(true);
    when(assetModelValidator.supports(any())).thenReturn(true);
    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
  }

  @Test
  void testEnumerateJobs() throws Exception {
    when(jobService.enumerateJobs(anyInt(), anyInt(), any(), any(), any()))
        .thenReturn(List.of(JOB_1, JOB_2));
    mvc.perform(get(ENUMERATE_JOBS_ENDPOINT))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()").value(2));
    verify(jobService, times(1))
        .enumerateJobs(eq(0), eq(10), any(), eq(SqlSortDirection.DESC), isNull());
  }

  @Test
  void testEnumerateJobsWithFilter() throws Exception {
    when(jobService.enumerateJobs(anyInt(), anyInt(), any(), any(), any()))
        .thenReturn(List.of(JOB_1, JOB_2));
    mvc.perform(get(ENUMERATE_JOBS_ENDPOINT).queryParam("className", FLIGHT_CLASS))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.length()").value(2));
    verify(jobService, times(1))
        .enumerateJobs(eq(0), eq(10), any(), eq(SqlSortDirection.DESC), eq(FLIGHT_CLASS));
  }

  @Test
  void testEnumerateJobsBadOffsetAndLimit() throws Exception {
    mvc.perform(get(ENUMERATE_JOBS_ENDPOINT).param("offset", "-1"))
        .andExpect(status().is4xxClientError())
        .andExpect(jsonPath("$.message").value("Offset must be greater than or equal to 0."));

    mvc.perform(get(ENUMERATE_JOBS_ENDPOINT).param("limit", "-1"))
        .andExpect(status().is4xxClientError())
        .andExpect(jsonPath("$.message").value("Limit must be greater than or equal to 1."));

    mvc.perform(get(ENUMERATE_JOBS_ENDPOINT).param("offset", "-1").param("limit", "-1"))
        .andExpect(status().is4xxClientError())
        .andExpect(
            jsonPath("$.message")
                .value(
                    "Offset must be greater than or equal to 0. Limit must be greater than or equal to 1."));
  }

  @Test
  void testRetrieveJob() throws Exception {
    when(jobService.retrieveJob(anyString(), any())).thenReturn(JOB_1);
    mvc.perform(get(RETRIEVE_JOB_ENDPOINT, JOB_1.getId()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value(JOB_1.getId()));
    verify(jobService, times(1)).retrieveJob(eq(JOB_1.getId()), any());
  }

  @Test
  void testRetrieveJobResult() throws Exception {
    record ResultClass(String value) {}
    ResultClass result = new ResultClass("fooResult");
    when(jobService.retrieveJobResult(anyString(), any(), any()))
        .thenReturn(new JobResultWithStatus<>().result(result).statusCode(HttpStatus.OK));
    mvc.perform(get(RETRIEVE_JOB_RESULT_ENDPOINT, JOB_2.getId()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.value").value("fooResult"));
    verify(jobService, times(1)).retrieveJobResult(eq(JOB_2.getId()), eq(Object.class), any());
  }
}
