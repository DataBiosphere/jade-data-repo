package scripts.testscripts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadResultModel;
import bio.terra.datarepo.model.JobModel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.BulkLoadUtils;
import scripts.utils.DataRepoUtils;

public class BulkLoad extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(BulkLoad.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public BulkLoad() {
    super();
    manipulatesKubernetes = true; // this test script manipulates Kubernetes
  }

  private int filesToLoad;

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load in the parameters list");
    } else {
      filesToLoad = Integer.parseInt(parameters.get(0));
    }
  }

  // The purpose of this test is to measure scaling of bulk load.
  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // set up and start bulk load job
    BulkLoadArrayRequestModel arrayLoad =
        BulkLoadUtils.buildBulkLoadFileRequest(filesToLoad, billingProfileModel.getId());
    JobModel bulkLoadArrayJobResponse =
        repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), arrayLoad);

    // wait for the job to complete and print out results to debug log level
    BulkLoadResultModel loadSummary =
        BulkLoadUtils.getAndDisplayResults(repositoryApi, bulkLoadArrayJobResponse, testUser);

    assertThat(
        "Number of successful files loaded should equal total files.",
        loadSummary.getTotalFiles(),
        equalTo(loadSummary.getSucceededFiles()));
  }
}
