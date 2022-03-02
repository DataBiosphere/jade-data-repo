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
import bio.terra.datarepo.model.SnapshotSummaryModel;
import com.google.cloud.storage.BlobId;
import common.utils.FileUtils;
import common.utils.StorageUtils;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.BulkLoadUtils;
import scripts.utils.DataRepoUtils;

public class BuildSnapshotWithFiles extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(BuildSnapshotWithFiles.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public BuildSnapshotWithFiles() {
    super();
  }

  private int filesToLoad;
  private int snapshotsToCreate;
  List<Integer> batchSizes = new ArrayList<>();
  List<SnapshotSummaryModel> snapshotSummaryModels = new ArrayList<>();
  private static List<BlobId> scratchFiles = new ArrayList<>();

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() < 3) {
      throw new IllegalArgumentException(
          "Required parameters: number of files, number of snapshots to create, batch size, batch size...");
    } else {
      filesToLoad = Integer.parseInt(parameters.get(0));
      snapshotsToCreate = Integer.parseInt(parameters.get(1));
      for (int i = 2; i < parameters.size(); i++) {
        batchSizes.add(Integer.parseInt(parameters.get(i)));
      }
      logger.info("Load {} files; create {} snapshots", filesToLoad, snapshotsToCreate);
    }
  }

  // The purpose of this test is to measure the performance of building the snapshot file system
  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // set up and start bulk load job of small local files
    BulkLoadArrayRequestModel arrayLoad =
        BulkLoadUtils.buildBulkLoadFileRequest100B(filesToLoad, billingProfileModel.getId());
    JobModel bulkLoadArrayJobResponse =
        repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), arrayLoad);

    // wait for the job to complete
    bulkLoadArrayJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, bulkLoadArrayJobResponse, testUser);
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
        DataRepoUtils.waitForJobToFinish(repositoryApi, ingestTabularDataJobResponse, testUser);
    IngestResponseModel ingestResponse =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestTabularDataJobResponse, IngestResponseModel.class);
    logger.info("Successfully loaded data into dataset: {}", ingestResponse.getDataset());

    for (Integer batchSize : batchSizes) {
      logger.info("Setting batch size to {}", batchSize);
      DataRepoUtils.setConfigParameter(repositoryApi, "FIRESTORE_SNAPSHOT_BATCH_SIZE", batchSize);

      // create the snapshots with all of the files
      for (int i = 0; i < snapshotsToCreate; i++) {
        JobModel createSnapshotJobResponse =
            DataRepoUtils.createSnapshot(
                repositoryApi, datasetSummaryModel, "snapshot-simple.json", testUser, true);

        SnapshotSummaryModel snapshotSummaryModel =
            DataRepoUtils.expectJobSuccess(
                repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);
        logger.info("Successfully created snapshot: {}", snapshotSummaryModel.getName());
        snapshotSummaryModels.add(snapshotSummaryModel);
      }
    }
  }

  @Override
  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    TestUserSpecification testUser = testUsers.get(0);
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    for (SnapshotSummaryModel snapshotSummaryModel : snapshotSummaryModels) {
      JobModel deleteSnapshotJobResponse =
          repositoryApi.deleteSnapshot(snapshotSummaryModel.getId());
      deleteSnapshotJobResponse =
          DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse, testUser);
      DataRepoUtils.expectJobSuccess(
          repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
      logger.info("Successfully deleted snapshot: {}", snapshotSummaryModel.getName());
    }

    // delete the scratch files used for ingesting tabular data
    StorageUtils.deleteFiles(
        StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);

    super.cleanup(testUsers);
  }
}
