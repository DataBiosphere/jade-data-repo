package bio.terra.service.job;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.StairwayJdbcConfiguration;
import bio.terra.app.usermetrics.BardClient;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.SqlSortDirection;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.exception.StairwayException;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "unittest"})
@Tag("bio.terra.common.category.Unit")
@EmbeddedDatabaseTest
public class JobServiceTest {
  private static final Logger logger = LoggerFactory.getLogger(JobServiceTest.class);

  private AuthenticatedUserRequest testUser =
      AuthenticatedUserRequest.builder()
          .setSubjectId("StairwayUnit")
          .setEmail("stairway@unit.com")
          .setToken("token")
          .build();

  private AuthenticatedUserRequest testUser2 =
      AuthenticatedUserRequest.builder()
          .setSubjectId("StairwayUnit2")
          .setEmail("stairway@unit2.com")
          .setToken("token")
          .build();

  private AuthenticatedUserRequest adminUser =
      AuthenticatedUserRequest.builder()
          .setSubjectId("StairwayUnit3")
          .setEmail("stairway@unit3.com")
          .setToken("token")
          .build();

  private final List<String> jobIds = new ArrayList<>();

  @Autowired private StairwayJdbcConfiguration stairwayJdbcConfiguration;

  @Autowired private JobService jobService;

  @Autowired private ApplicationConfiguration appConfig;

  @MockBean private IamService samService;

  @MockBean private BardClient bardClient;

  @BeforeEach
  public void setUp() throws Exception {
    when(samService.isAuthorized(
            testUser, IamResourceType.DATAREPO, appConfig.getResourceId(), IamAction.LIST_JOBS))
        .thenReturn(false);
    when(samService.isAuthorized(
            testUser2, IamResourceType.DATAREPO, appConfig.getResourceId(), IamAction.LIST_JOBS))
        .thenReturn(false);
    when(samService.isAuthorized(
            adminUser, IamResourceType.DATAREPO, appConfig.getResourceId(), IamAction.LIST_JOBS))
        .thenReturn(true);
    jobIds.clear();
  }

  @AfterEach
  public void tearDown() throws Exception {
    logger.info("Deleting {} jobs", jobIds.size());
    jobIds.forEach(this::deleteJob);
  }

  @Test
  public void enumerateTooLongBackFilterTest() throws Exception {
    int numVisibleJobs = 3;
    List<JobModel> expectedJobs =
        IntStream.range(0, numVisibleJobs)
            .mapToObj(
                i ->
                    new JobModel()
                        .id(runFlight(makeDescription(i), makeFlightClass(i), testUser))
                        .jobStatus(JobStatusEnum.SUCCEEDED)
                        .statusCode(HttpStatus.I_AM_A_TEAPOT.value())
                        .description(makeDescription(i))
                        .className(makeFlightClass(i).getName()))
            .toList();
    assertThat(
        "All jobs are returned",
        jobService.enumerateJobs(0, 100, testUser, SqlSortDirection.ASC, ""),
        contains(getJobMatchers(expectedJobs)));

    // Update the submission time of the first job to be a long time ago
    updateJobSubmissionTime(
        expectedJobs.get(0).getId(),
        Instant.now().minus(Duration.ofDays(appConfig.getMaxNumberOfDaysToShowJobs() + 1)));
    assertThat(
        "The first job is not returned anymore",
        jobService.enumerateJobs(0, 100, testUser, SqlSortDirection.ASC, ""),
        contains(getJobMatchers(expectedJobs.subList(1, numVisibleJobs))));
  }

  @Test
  public void retrieveTest() throws Exception {
    // We perform 7 flights of alternating classes and then retrieve and enumerate them.
    // The fids list should be in exactly the same order as the database ordered by submit time.

    List<JobModel> expectedJobs =
        IntStream.range(0, 7)
            .mapToObj(
                i ->
                    new JobModel()
                        .id(runFlight(makeDescription(i), makeFlightClass(i), testUser))
                        .jobStatus(JobStatusEnum.SUCCEEDED)
                        .statusCode(HttpStatus.I_AM_A_TEAPOT.value())
                        .description(makeDescription(i))
                        .className(makeFlightClass(i).getName()))
            .toList();

    // Test single retrieval
    testSingleRetrieval(expectedJobs.get(2));

    // Test result retrieval - the body should be the description string
    testResultRetrieval(expectedJobs.get(2));

    // Retrieve everything
    assertThat(
        "retrieve everything",
        jobService.enumerateJobs(0, 100, testUser, SqlSortDirection.ASC, ""),
        contains(getJobMatchers(expectedJobs)));

    // Retrieve the middle 3; offset means skip 2 rows
    assertThat(
        "retrieve the middle three",
        jobService.enumerateJobs(2, 3, testUser, SqlSortDirection.ASC, ""),
        contains(getJobMatchers(expectedJobs.subList(2, 5))));

    // Retrieve in descending order and filtering to the even (JobServiceTestFlight) flights
    assertThat(
        "retrieve descending and alternating",
        jobService.enumerateJobs(
            0, 4, null, SqlSortDirection.DESC, JobServiceTestFlight.class.getName()),
        contains(
            getJobMatcher(expectedJobs.get(6)),
            getJobMatcher(expectedJobs.get(4)),
            getJobMatcher(expectedJobs.get(2)),
            getJobMatcher(expectedJobs.get(0))));

    // Retrieve from the end; should only get the last one back
    assertThat(
        "retrieve from the end",
        jobService.enumerateJobs(6, 3, testUser, SqlSortDirection.ASC, ""),
        contains(getJobMatcher((expectedJobs.get(6)))));

    // Retrieve past the end; should get nothing
    assertThat(
        "retrieve from the end",
        jobService.enumerateJobs(22, 3, testUser, SqlSortDirection.ASC, ""),
        is(List.of()));
  }

  @Test
  public void enumerateJobsPermissionTest() throws Exception {
    // We perform 9 flights of alternating classes and then retrieve and enumerate them.
    // The fids list should be in exactly the same order as the database ordered by submit time.

    List<JobModel> allJobs = new ArrayList<>();

    // Launch 2 jobs that are not visible to the second test user
    UUID privateDatasetId = UUID.randomUUID();
    for (int i = 0; i < 2; i++) {
      allJobs.add(
          runFlightWithParameters(
              makeDescription(i), makeFlightClass(i), testUser, String.valueOf(privateDatasetId)));
    }

    // Initially, testUser2 has no access to any datasets
    when(samService.listAuthorizedResources(testUser2, IamResourceType.DATASET))
        .thenReturn(Map.of());

    assertThat(
        "jobs are visible to testUser",
        jobService.enumerateJobs(0, 100, testUser, SqlSortDirection.ASC, ""),
        contains(allJobs.toArray(new JobModel[0])));

    assertTrue(
        "no jobs are visible to testUser2",
        jobService.enumerateJobs(0, 100, testUser2, SqlSortDirection.ASC, "").isEmpty());

    // Launch 2 jobs that are visible to the second test user via dataset access
    UUID sharedDatasetId = UUID.randomUUID();
    for (int i = 0; i < 2; i++) {
      allJobs.add(
          runFlightWithParameters(
              makeDescription(i), makeFlightClass(i), testUser, String.valueOf(sharedDatasetId)));
    }

    // Now testUser2 has access to the shared dataset
    when(samService.listAuthorizedResources(testUser2, IamResourceType.DATASET))
        .thenReturn(Map.of(sharedDatasetId, Set.of(IamRole.STEWARD, IamRole.CUSTODIAN)));

    // Launch 5 jobs as the testUser2
    for (int i = 0; i < 5; i++) {
      allJobs.add(
          runFlightWithParameters(
              makeDescription(i), makeFlightClass(i), testUser2, String.valueOf(sharedDatasetId)));
    }

    assertThat(
        "only user-launched jobs are visible to testsUser",
        jobService.enumerateJobs(0, 100, testUser, SqlSortDirection.ASC, ""),
        contains(allJobs.subList(0, 4).toArray(new JobModel[0])));

    assertThat(
        "shared and user-launched jobs are visible to testsUser2",
        jobService.enumerateJobs(0, 100, testUser2, SqlSortDirection.ASC, ""),
        contains(allJobs.subList(2, 9).toArray(new JobModel[0])));

    // Retrieve the middle 3; offset means skip 2 rows
    assertThat(
        "retrieve the middle three",
        jobService.enumerateJobs(2, 3, testUser2, SqlSortDirection.ASC, ""),
        contains(allJobs.subList(4, 7).toArray(new JobModel[0])));

    // Retrieve in descending order and filtering to the even (JobServiceTestFlight) flights
    assertThat(
        "retrieve descending and alternating",
        jobService.enumerateJobs(
            0, 4, testUser2, SqlSortDirection.DESC, JobServiceTestFlight.class.getName()),
        contains(allJobs.get(8), allJobs.get(6), allJobs.get(4), allJobs.get(2)));

    // Retrieve from the end; should only get the last one back
    assertThat(
        "retrieve from the end",
        jobService.enumerateJobs(6, 3, testUser2, SqlSortDirection.ASC, ""),
        contains(allJobs.get(8)));

    // Retrieve past the end; should get nothing
    assertTrue(
        "retrieve from the end",
        jobService.enumerateJobs(22, 3, testUser2, SqlSortDirection.ASC, "").isEmpty());

    assertThat(
        "admin user can list all jobs",
        jobService.enumerateJobs(0, 100, adminUser, SqlSortDirection.ASC, ""),
        contains(allJobs.toArray(new JobModel[0])));
  }

  private void testSingleRetrieval(JobModel job) throws InterruptedException {
    JobModel response = jobService.retrieveJob(job.getId(), null);
    assertThat(response, notNullValue());
    assertThat(response, getJobMatcher(job));
  }

  private void testResultRetrieval(JobModel job) throws InterruptedException {
    JobService.JobResultWithStatus<String> resultHolder =
        jobService.retrieveJobResult(job.getId(), String.class, null);
    assertThat(resultHolder.getStatusCode(), is(equalTo(HttpStatus.I_AM_A_TEAPOT)));
    assertThat(resultHolder.getResult(), is(equalTo(job.getDescription())));
  }

  @Test
  public void testBadIdRetrieveJob() throws InterruptedException {
    assertThrows(StairwayException.class, () -> jobService.retrieveJob("abcdef", null));
  }

  @Test
  public void testBadIdRetrieveResult() throws InterruptedException {
    assertThrows(
        StairwayException.class, () -> jobService.retrieveJobResult("abcdef", Object.class, null));
  }

  private Matcher<JobModel> getJobMatcher(JobModel jobModel) {
    return samePropertyValuesAs(jobModel, "submitted", "completed");
  }

  @SuppressWarnings("unchecked")
  private Matcher<JobModel>[] getJobMatchers(List<JobModel> jobModels) {
    return jobModels.stream().map(this::getJobMatcher).toArray(Matcher[]::new);
  }

  // Submit a flight; wait for it to finish; return the flight id
  private String runFlight(
      String description, Class<? extends Flight> clazz, AuthenticatedUserRequest testUser) {
    String jobId = jobService.newJob(description, clazz, null, testUser).submit();
    // Poll repeatedly with no breaks: we expect the job to complete quickly.
    jobService.waitForJob(jobId, 0);
    jobIds.add(jobId);
    return jobId;
  }

  private JobModel runFlightWithParameters(
      String description,
      Class<? extends Flight> clazz,
      AuthenticatedUserRequest testUser,
      String resourceId) {
    String jobId =
        jobService
            .newJob(description, clazz, null, testUser)
            .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
            .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), resourceId)
            .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
            .submit();
    // Poll repeatedly with no breaks: we expect the job to complete quickly.
    jobService.waitForJob(jobId, 0);
    jobIds.add(jobId);
    return jobService.retrieveJob(jobId, testUser);
  }

  private String makeDescription(int ii) {
    return String.format("flight%d", ii);
  }

  private Class<? extends Flight> makeFlightClass(int ii) {
    return ii % 2 == 0 ? JobServiceTestFlight.class : JobServiceTestFlightAlt.class;
  }

  /**
   * There's not a way to set the submission time (which is reasonable) so we fake it by setting the
   * submission time artificially
   *
   * @param jobId The job id to update
   */
  private void updateJobSubmissionTime(String jobId, Instant submitTime) {
    logger.info("- Updating job {} submission time", jobId);
    NamedParameterJdbcTemplate jdbcTemplate =
        new NamedParameterJdbcTemplate(stairwayJdbcConfiguration.getDataSource());

    String sql = """
update flight
set submit_time=:submit_time
where flightid=:id
""";

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", jobId)
            .addValue("submit_time", Date.from(submitTime));
    jdbcTemplate.update(sql, params);
  }

  /**
   * Would be great to have a method in stairway for this but in the meantime, adding this to the
   * test code since we don't want this to ever run against a running instance
   */
  private void deleteJob(String jobId) {
    logger.info("- Removing job {}", jobId);
    NamedParameterJdbcTemplate jdbcTemplate =
        new NamedParameterJdbcTemplate(stairwayJdbcConfiguration.getDataSource());

    String sql =
        """
delete from flightworking where flightlog_id in (select id from flightlog where flightid=:id);
delete from flightlog where flightid=:id;
delete from flightinput where flightid=:id;
delete from flight where flightid=:id;
""";

    MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", jobId);
    jdbcTemplate.update(sql, params);
  }
}
