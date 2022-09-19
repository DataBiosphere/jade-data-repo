package bio.terra.service.job;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.StairwayJdbcConfiguration;
import bio.terra.app.usermetrics.BardClient;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.exception.StairwayException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
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
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
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

  @Before
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

  @After
  public void tearDown() throws Exception {
    logger.info("Deleting {} jobs", jobIds.size());
    jobIds.forEach(this::deleteJob);
  }

  @Test
  public void retrieveTest() throws Exception {
    // We perform 7 flights of alternating classes and then retrieve and enumerate them.
    // The fids list should be in exactly the same order as the database ordered by submit time.

    List<JobModel> expectedJobs = new ArrayList<>();

    for (int i = 0; i < 7; i++) {
      String jobId = runFlight(makeDescription(i), makeFlightClass(i), testUser);
      expectedJobs.add(
          new JobModel()
              .id(jobId)
              .jobStatus(JobStatusEnum.SUCCEEDED)
              .statusCode(HttpStatus.I_AM_A_TEAPOT.value())
              .description(makeDescription(i))
              .className(makeFlightClass(i).getName()));
    }

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

    // Launch 2 jobs that are not visible to the test user
    UUID privateDatasetId = UUID.randomUUID();
    for (int i = 0; i < 2; i++) {
      runFlightWithParameters(
          makeDescription(i), makeFlightClass(i), testUser, String.valueOf(privateDatasetId));
    }
    when(samService.listActions(
            testUser2, IamResourceType.DATASET, String.valueOf(privateDatasetId)))
        .thenReturn(List.of());

    assertEquals(
        "no jobs are visible",
        jobService.enumerateJobs(0, 100, testUser2, SqlSortDirection.ASC, "").size(),
        0);

    // Launch 2 jobs that are visible to the test user
    List<JobModel> accessibleJobs = new ArrayList<>();
    UUID sharedDatasetId = UUID.randomUUID();
    for (int i = 0; i < 2; i++) {
      String jobId =
          runFlightWithParameters(
              makeDescription(i), makeFlightClass(i), testUser, String.valueOf(sharedDatasetId));
      accessibleJobs.add(
          new JobModel()
              .id(jobId)
              .jobStatus(JobStatusEnum.SUCCEEDED)
              .statusCode(HttpStatus.I_AM_A_TEAPOT.value())
              .description(makeDescription(i))
              .className(makeFlightClass(i).getName()));
    }

    when(samService.listActions(
            testUser2, IamResourceType.DATASET, String.valueOf(sharedDatasetId)))
        .thenReturn(List.of("ingest_data"));

    // Launch 5 jobs as the test user
    for (int i = 0; i < 5; i++) {
      String jobId =
          runFlightWithParameters(
              makeDescription(i), makeFlightClass(i), testUser2, String.valueOf(sharedDatasetId));
      accessibleJobs.add(
          new JobModel()
              .id(jobId)
              .jobStatus(JobStatusEnum.SUCCEEDED)
              .statusCode(HttpStatus.I_AM_A_TEAPOT.value())
              .description(makeDescription(i))
              .className(makeFlightClass(i).getName()));
    }

    assertThat(
        "shared and user-launched jobs are visible",
        jobService.enumerateJobs(0, 100, testUser2, SqlSortDirection.ASC, ""),
        contains(getJobMatchers(accessibleJobs)));

    // Retrieve the middle 3; offset means skip 2 rows
    assertThat(
        "retrieve the middle three",
        jobService.enumerateJobs(2, 3, testUser2, SqlSortDirection.ASC, ""),
        contains(getJobMatchers(accessibleJobs.subList(2, 5))));

    // Retrieve in descending order and filtering to the even (JobServiceTestFlight) flights
    assertThat(
        "retrieve descending and alternating",
        jobService.enumerateJobs(
            0, 4, testUser2, SqlSortDirection.DESC, JobServiceTestFlight.class.getName()),
        contains(
            getJobMatcher(accessibleJobs.get(6)),
            getJobMatcher(accessibleJobs.get(4)),
            getJobMatcher(accessibleJobs.get(2)),
            getJobMatcher(accessibleJobs.get(0))));

    // Retrieve from the end; should only get the last one back
    assertThat(
        "retrieve from the end",
        jobService.enumerateJobs(6, 3, testUser2, SqlSortDirection.ASC, ""),
        contains(getJobMatcher((accessibleJobs.get(6)))));

    // Retrieve past the end; should get nothing
    assertThat(
        "retrieve from the end",
        jobService.enumerateJobs(22, 3, testUser2, SqlSortDirection.ASC, ""),
        is(List.of()));

    assertEquals(
        "admin user can list all jobs",
        jobService.enumerateJobs(0, 100, adminUser, SqlSortDirection.ASC, "").size(),
        9);
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

  @Test(expected = StairwayException.class)
  public void testBadIdRetrieveJob() throws InterruptedException {
    jobService.retrieveJob("abcdef", null);
  }

  @Test(expected = StairwayException.class)
  public void testBadIdRetrieveResult() throws InterruptedException {
    jobService.retrieveJobResult("abcdef", Object.class, null);
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
    jobService.waitForJob(jobId);
    jobIds.add(jobId);
    return jobId;
  }

  private String runFlightWithParameters(
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
    jobService.waitForJob(jobId);
    jobIds.add(jobId);
    return jobId;
  }

  private String makeDescription(int ii) {
    return String.format("flight%d", ii);
  }

  private Class<? extends Flight> makeFlightClass(int ii) {
    return ii % 2 == 0 ? JobServiceTestFlight.class : JobServiceTestFlightAlt.class;
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
