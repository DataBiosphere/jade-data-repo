package bio.terra.service.job;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.samePropertyValuesAs;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.model.SqlSortDirection;
import bio.terra.stairway.Flight;
import bio.terra.stairway.exception.StairwayException;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
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

  @Autowired private JobService jobService;

  @Autowired private ApplicationConfiguration appConfig;

  @Test
  public void retrieveTest() throws Exception {
    // We perform 7 flights of alternating classes and then retrieve and enumerate them.
    // The fids list should be in exactly the same order as the database ordered by submit time.

    List<JobModel> expectedJobs = new ArrayList<>();

    for (int i = 0; i < 7; i++) {
      String jobId = runFlight(makeDescription(i), makeFlightClass(i));
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
        jobService.enumerateJobs(0, 100, null, SqlSortDirection.ASC, ""),
        contains(getJobMatchers(expectedJobs)));

    // Retrieve the middle 3; offset means skip 2 rows
    assertThat(
        "retrieve the middle three",
        jobService.enumerateJobs(2, 3, null, SqlSortDirection.ASC, ""),
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
        jobService.enumerateJobs(6, 3, null, SqlSortDirection.ASC, ""),
        contains(getJobMatcher((expectedJobs.get(6)))));

    // Retrieve past the end; should get nothing
    assertThat(
        "retrieve from the end",
        jobService.enumerateJobs(22, 3, null, SqlSortDirection.ASC, ""),
        is(List.of()));
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
  private String runFlight(String description, Class<? extends Flight> clazz) {
    String jobId = jobService.newJob(description, clazz, null, testUser).submit();
    jobService.waitForJob(jobId);
    return jobId;
  }

  private String makeDescription(int ii) {
    return String.format("flight%d", ii);
  }

  private Class<? extends Flight> makeFlightClass(int ii) {
    return ii % 2 == 0 ? JobServiceTestFlight.class : JobServiceTestFlightAlt.class;
  }
}
