package bio.terra.service.job;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.UsersBase;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.JobModel;
import org.junit.Before;
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
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class JobResponseTest extends UsersBase {

  private static Logger logger = LoggerFactory.getLogger(JobResponseTest.class);

  @Autowired private DataRepoClient dataRepoClient;

  @Autowired private TestConfiguration testConfiguration;

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @Test
  public void testJobResultResponse() throws Exception {
    BillingProfileRequestModel billingProfileRequestModel =
        ProfileFixtures.billingProfileRequest(
            ProfileFixtures.billingProfileForAccount(
                testConfiguration.getGoogleBillingAccountId()));
    String json = TestUtils.mapToJson(billingProfileRequestModel);

    DataRepoResponse<JobModel> jobResponse =
        dataRepoClient.post(steward(), "/api/resources/v1/profiles", json, JobModel.class);
    assertTrue("profile create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "profile create launch response is present", jobResponse.getResponseObject().isPresent());

    DataRepoResponse<BillingProfileModel> postResponse =
        dataRepoClient.waitForResponse(steward(), jobResponse, BillingProfileModel.class);

    assertThat(
        "billing profile model is successfully created",
        postResponse.getStatusCode(),
        equalTo(HttpStatus.CREATED));

    //    int sleepSeconds = 1;
    //    String location = dataRepoClient.getLocationHeader(jobResponse);
    //    try {
    //      while (true) {
    //        TimeUnit.SECONDS.sleep(sleepSeconds);
    //        DataRepoResponse<JobModel> jobModelResponse =
    //            dataRepoClient.get(steward(), location, JobModel.class);
    //        logger.info(
    //            "Got response. status: "
    //                + jobModelResponse.getStatusCode()
    //                + " location: "
    //                + jobModelResponse.getLocationHeader().orElse("not present"));
    //      }
    //    } catch (InterruptedException ex) {
    //      logger.info("interrupted ex: " + ex.getMessage(), ex);
    //      throw new IllegalStateException("unexpected interrupt waiting for response", ex);
    //    }
  }
}
