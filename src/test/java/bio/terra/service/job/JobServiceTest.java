package bio.terra.service.job;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.model.JobModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.stairway.exception.StairwayException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@Category(Unit.class)
public class JobServiceTest {
  private static final Logger logger = LoggerFactory.getLogger(JobServiceTest.class);

  private AuthenticatedUserRequest testUser =
      new AuthenticatedUserRequest()
          .subjectId("StairwayUnit")
          .email("stairway@unit.com")
          .token(Optional.empty());

  @Autowired private JobService jobService;

  @Autowired private ApplicationConfiguration appConfig;

  // This test is unreliable. See https://broadworkbench.atlassian.net/browse/DR-1248
  // todo - fix w/ DR-1255
  @Ignore
  @Test
  public void retrieveTest() throws Exception {
    // We perform 7 flights and then retrieve and enumerate them.
    // The fids list should be in exactly the same order as the database ordered by submit time.

    List<String> jobIds = new ArrayList<>();
    List<ResourceAndAccessPolicy> allowedIds = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      String jobId = runFlight(makeDescription(i));
      jobIds.add(jobId);
      allowedIds.add(new ResourceAndAccessPolicy().resourceId(jobId));
    }

    // Test single retrieval
    testSingleRetrieval(jobIds);

    // Test result retrieval - the body should be the description string
    testResultRetrieval(jobIds);

    // Retrieve everything
    testEnumRange(jobIds, 0, 100, allowedIds);

    // Retrieve the middle 3; offset means skip 2 rows
    testEnumRange(jobIds, 2, 3, allowedIds);

    // Retrieve from the end; should only get the last one back
    testEnumCount(1, 6, 3, allowedIds);

    // Retrieve past the end; should get nothing
    testEnumCount(0, 22, 3, allowedIds);
  }

  private void testSingleRetrieval(List<String> fids) throws InterruptedException {
    JobModel response = jobService.retrieveJob(fids.get(2), null);
    Assert.assertNotNull(response);
    validateJobModel(response, 2, fids);
  }

  private void testResultRetrieval(List<String> fids) throws InterruptedException {
    JobService.JobResultWithStatus<String> resultHolder =
        jobService.retrieveJobResult(fids.get(2), String.class, null);
    Assert.assertThat(resultHolder.getStatusCode(), is(equalTo(HttpStatus.I_AM_A_TEAPOT)));
    Assert.assertThat(resultHolder.getResult(), is(equalTo(makeDescription(2))));
  }

  // Get some range and compare it with the fids
  private void testEnumRange(
      List<String> fids, int offset, int limit, List<ResourceAndAccessPolicy> resourceIds)
      throws InterruptedException {

    List<JobModel> jobList = jobService.enumerateJobs(offset, limit, null);
    Assert.assertNotNull(jobList);
    int index = offset;
    for (JobModel job : jobList) {
      validateJobModel(job, index, fids);
      index++;
    }
  }

  // Get some range and make sure we got the number we expected
  private void testEnumCount(
      int count, int offset, int length, List<ResourceAndAccessPolicy> resourceIds)
      throws InterruptedException {

    List<JobModel> jobList = jobService.enumerateJobs(offset, length, null);
    Assert.assertNotNull(jobList);
    Assert.assertThat(jobList.size(), is(count));
  }

  @Test(expected = StairwayException.class)
  public void testBadIdRetrieveJob() throws InterruptedException {
    jobService.retrieveJob("abcdef", null);
  }

  @Test(expected = StairwayException.class)
  public void testBadIdRetrieveResult() throws InterruptedException {
    jobService.retrieveJobResult("abcdef", Object.class, null);
  }

  private void validateJobModel(JobModel jm, int index, List<String> fids) {
    Assert.assertThat(jm.getDescription(), is(equalTo(makeDescription(index))));
    Assert.assertThat(jm.getId(), is(equalTo(fids.get(index))));
    Assert.assertThat(jm.getJobStatus(), is(JobModel.JobStatusEnum.SUCCEEDED));
    Assert.assertThat(jm.getStatusCode(), is(HttpStatus.I_AM_A_TEAPOT.value()));
  }

  // Submit a flight; wait for it to finish; return the flight id
  private String runFlight(String description) throws InterruptedException {
    String jobId =
        jobService.newJob(description, JobServiceTestFlight.class, null, testUser).submit();
    jobService.waitForJob(jobId);
    return jobId;
  }

  private String makeDescription(int ii) {
    return String.format("flight%d", ii);
  }
}
