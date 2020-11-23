package scripts.testscripts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadResultModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import bio.terra.testrunner.common.utils.FileUtils;
import bio.terra.testrunner.common.utils.StorageUtils;
import bio.terra.testrunner.runner.config.TestUserSpecification;
import com.google.cloud.storage.BlobId;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.BulkLoadUtils;
import scripts.utils.DataRepoUtils;

public class CreateSnapshot extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(CreateSnapshot.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public CreateSnapshot() {
    super();
  }

  private SnapshotModel snapshotModel;

  private static List<BlobId> scratchFiles = new ArrayList<>();

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // create the profile and dataset
    super.setup(testUsers);

    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // Be careful testing this at larger sizes
    DataRepoUtils.setConfigParameter(repositoryApi, "LOAD_BULK_ARRAY_FILES_MAX", filesToLoad);

    // set up and start bulk load job of small local files
    BulkLoadArrayRequestModel arrayLoad =
        BulkLoadUtils.buildBulkLoadFileRequest100B(filesToLoad, billingProfileModel.getId());
    JobModel bulkLoadArrayJobResponse =
        repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), arrayLoad);

    // wait for the job to complete
    bulkLoadArrayJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, bulkLoadArrayJobResponse);
    BulkLoadArrayResultModel result =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, bulkLoadArrayJobResponse, BulkLoadArrayResultModel.class);
    BulkLoadResultModel loadSummary = result.getLoadSummary();
    assertThat(
        "Number of successful files loaded should equal total files.",
        loadSummary.getTotalFiles(),
        equalTo(loadSummary.getSucceededFiles()));

    // generate load for the simple dataset
    String testConfigGetIngestbucket = "jade-testdata";
    String fileRefName =
        "scratch/buildSnapshotWithFiles/" + FileUtils.randomizeName("input") + ".json";
    BlobId scratchFile =
        BulkLoadUtils.writeScratchFileForIngestRequest(
            server.testRunnerServiceAccount, result, testConfigGetIngestbucket, fileRefName);
    IngestRequestModel ingestRequest = BulkLoadUtils.makeIngestRequestFromScratchFile(scratchFile);
    scratchFiles.add(scratchFile); // make sure the scratch file gets cleaned up later

    // load the data
    JobModel ingestTabularDataJobResponse =
        repositoryApi.ingestDataset(datasetSummaryModel.getId(), ingestRequest);

    ingestTabularDataJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, ingestTabularDataJobResponse);
    IngestResponseModel ingestResponse =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestTabularDataJobResponse, IngestResponseModel.class);
    logger.info("Successfully loaded data into dataset: {}", ingestResponse.getDataset());
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

  public void userJourney(TestUserSpecification testUser) throws Exception {
    try {
      logger.info("Creating a snapshot");
      // get the ApiClient for the snapshot creator, same as the dataset creator
      ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(testUser, server);
      RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);
      // make the create snapshot request and wait for the job to finish
      JobModel createSnapshotJobResponse =
          DataRepoUtils.createSnapshot(
              repositoryApi, datasetSummaryModel, "snapshot-simple.json", true);

      logger.info("Snapshot job is done");
      if (createSnapshotJobResponse.getJobStatus() == JobModel.JobStatusEnum.FAILED) {
        throw new RuntimeException("Snapshot job did not finish successfully");
      }
      // save a reference to the snapshot summary model so we can delete it in cleanup()
      SnapshotSummaryModel snapshotSummaryModel =
          DataRepoUtils.expectJobSuccess(
              repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);
      logger.info("Successfully created snapshot: {}", snapshotSummaryModel.getName());

      // now go and retrieve the file Id that should be stored in the snapshot
      snapshotModel = repositoryApi.retrieveSnapshot(snapshotSummaryModel.getId());
    } catch (Exception e) {
      logger.error("Error in journey", e);
      e.printStackTrace();
      throw e;
    }
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    logger.info("Tearing down");

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    if (snapshotModel != null) {
      JobModel deleteSnapshotJobResponse = repositoryApi.deleteSnapshot(snapshotModel.getId());
      deleteSnapshotJobResponse =
          DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse);
      DataRepoUtils.expectJobSuccess(
          repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
      logger.info("Successfully deleted snapshot: {}", snapshotModel.getName());

      super.cleanup(testUsers);
      // delete the profile and dataset

      // delete the scratch files used for ingesting tabular data and soft delete rows
      StorageUtils.deleteFiles(
          StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);
    }
  }
}
