package scripts.testscripts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.BulkLoadResultModel;
import bio.terra.datarepo.model.CloudPlatform;
import bio.terra.datarepo.model.ErrorModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.JobModel;
import com.google.cloud.storage.BlobId;
import common.utils.StorageUtils;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.BulkLoadUtils;
import scripts.utils.DataRepoUtils;

public class CombinedIngestLoad extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(CombinedIngestLoad.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public CombinedIngestLoad() {
    super();
    //    manipulatesKubernetes = true; // this test script manipulates Kubernetes
  }

  private int filesToLoad;

  private static final List<BlobId> scratchFiles = new ArrayList<>();

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // create the profile and dataset
    super.setup(testUsers);

    // upload scratch files
    String testConfigGetIngestbucket = "jade-testdata";
    BulkLoadArrayRequestModel arrayRequestModel =
        BulkLoadUtils.buildBulkLoadFileRequest100B(filesToLoad, billingProfileModel.getId());
    String scratchFilePrefix = "bulkCombinedIngestTest/" + datasetSummaryModel.getId();

    for (int i = 0; i < arrayRequestModel.getLoadArray().size(); i++) {
      BulkLoadFileModel fileModel = arrayRequestModel.getLoadArray().get(i);
      String scratchFileName = String.format("%s/combined-ingest-%s.json", scratchFilePrefix, i);
      BlobId scratchFile =
          BulkLoadUtils.writeScratchFileForCombinedIngestRequest(
              server.testRunnerServiceAccount,
              fileModel,
              testConfigGetIngestbucket,
              scratchFileName);
      scratchFiles.add(scratchFile);
    }
  }

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load in the parameters list");
    } else {
      filesToLoad = Integer.parseInt(parameters.get(0));
    }
  }

  // The purpose of this test is to measure scaling of parallel combined file ingests.
  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // Launch combined ingest requests
    ArrayList<JobModel> responses = new ArrayList<>();
    for (BlobId scratchFile : scratchFiles) {
      IngestRequestModel ingestRequest =
          BulkLoadUtils.makeIngestRequestFromScratchFile(scratchFile);
      JobModel ingestTabularDataJobResponse =
          repositoryApi.ingestDataset(datasetSummaryModel.getId(), ingestRequest);
      responses.add(ingestTabularDataJobResponse);
    }

    // wait for the job to complete and print out results to debug log level
    ArrayList<String> errors = new ArrayList<>();
    for (JobModel jobResponse : responses) {
      JobModel finishedJob = DataRepoUtils.waitForJobToFinish(repositoryApi, jobResponse, testUser);
      if (finishedJob.getJobStatus().equals(JobModel.JobStatusEnum.FAILED)) {
        ErrorModel errorModel =
            DataRepoUtils.getJobResult(repositoryApi, jobResponse, ErrorModel.class);
        errors.add(errorModel.getMessage());
      } else {
        BulkLoadArrayResultModel result =
            DataRepoUtils.getJobResult(repositoryApi, jobResponse, BulkLoadArrayResultModel.class);
        BulkLoadResultModel loadSummary = result.getLoadSummary();
        assertThat(
            "Number of successful files loaded should equal total files.",
            loadSummary.getTotalFiles(),
            equalTo(loadSummary.getSucceededFiles()));
      }
      logger.info("Failed combined ingests: " + errors.size());
      assertThat("No errors present", errors.size(), equalTo(0));
    }
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    logger.info("Tearing down");

    // delete the profile and dataset
    super.cleanup(testUsers);

    // delete the scratch files used for ingesting metadata
    if (cloudPlatform.equals(CloudPlatform.GCP)) {
      StorageUtils.deleteFiles(
          StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);
    }
  }
}
