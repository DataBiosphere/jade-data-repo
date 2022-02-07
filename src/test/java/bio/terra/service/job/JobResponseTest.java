package bio.terra.service.job;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.UsersBase;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
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
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private JsonLoader jsonLoader;

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @Test
  public void testJobResultResponse() throws Exception {
    BillingProfileModel billingProfile = dataRepoFixtures.createBillingProfile(steward());

    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(
            steward(), billingProfile.getId(), "ingest-test-dataset.json");

    IngestRequestModel ingestRequest =
        dataRepoFixtures.buildSimpleIngest(
            "participant", "ingest-test/ingest-test-participant.json");
    IngestResponseModel ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetSummaryModel.getId(), ingestRequest);
    assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(5L));

    ingestRequest =
        dataRepoFixtures.buildSimpleIngest("sample", "ingest-test/ingest-test-sample.json");
    ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetSummaryModel.getId(), ingestRequest);
    assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(7L));

    ingestRequest = dataRepoFixtures.buildSimpleIngest("file", "ingest-test/ingest-test-file.json");
    ingestResponse =
        dataRepoFixtures.ingestJsonData(steward(), datasetSummaryModel.getId(), ingestRequest);
    assertThat("correct file row count", ingestResponse.getRowCount(), equalTo(1L));

    SnapshotRequestModel requestModel =
        jsonLoader.loadObject("ingest-test-snapshot.json", SnapshotRequestModel.class);
    requestModel.setName(Names.randomizeName(requestModel.getName()));

    requestModel.getContents().get(0).setDatasetName(datasetSummaryModel.getName());
    requestModel.setProfileId(billingProfile.getId());
    String json = TestUtils.mapToJson(requestModel);

    DataRepoResponse<JobModel> jobResponse =
        dataRepoClient.post(steward(), "/api/repository/v1/snapshots", json, JobModel.class, false);
    assertTrue("snapshot create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
    assertTrue(
        "snapshot create launch response is present", jobResponse.getResponseObject().isPresent());

    int sleepSeconds = 1;
    String location = dataRepoClient.getLocationHeader(jobResponse);
    try {
      while (true) {

        DataRepoResponse<JobModel> jobStatus =
            dataRepoClient.get(steward(), location, JobModel.class);
        logger.info("Job Status: {}", jobStatus.getResponseObject().get());
        try {
          DataRepoResponse<SnapshotSummaryModel> jobResult =
              dataRepoClient.get(steward(), location + "/result", SnapshotSummaryModel.class);
          logger.info("Job Result: {}", jobResult.getResponseObject());
        } catch (Exception ex) {
          logger.info("No result returned.");
        }
      }
    } catch (InterruptedException ex) {
      logger.info("interrupted ex: " + ex.getMessage(), ex);
      throw new IllegalStateException("unexpected interrupt waiting for response", ex);
    }
  }
}
