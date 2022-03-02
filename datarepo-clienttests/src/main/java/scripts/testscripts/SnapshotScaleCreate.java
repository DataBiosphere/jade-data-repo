package scripts.testscripts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadResultModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.IngestResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import com.google.cloud.storage.BlobId;
import common.utils.FileUtils;
import common.utils.StorageUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.BulkLoadUtils;
import scripts.utils.DataRepoUtils;

public class SnapshotScaleCreate extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotScaleCreate.class);
  private int numSnapshots = 2;
  private int filesToLoad = 1;
  private static List<BlobId> scratchFiles = new ArrayList<>();
  private List<SnapshotSummaryModel> snapshotList = new ArrayList<>();

  /** Public constructor so that this class can be instantiated via reflection. */
  public SnapshotScaleCreate() {}

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
        DataRepoUtils.waitForJobToFinish(repositoryApi, bulkLoadArrayJobResponse, datasetCreator);
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
        DataRepoUtils.waitForJobToFinish(
            repositoryApi, ingestTabularDataJobResponse, datasetCreator);
    IngestResponseModel ingestResponse =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestTabularDataJobResponse, IngestResponseModel.class);
    logger.info("Successfully loaded data into dataset: {}", ingestResponse.getDataset());
  }

  @Override
  public void setParameters(List<String> parameters) {
    if (parameters != null && parameters.size() > 0) {
      numSnapshots = Integer.parseInt(parameters.get(0));
    }
    logger.info("Number of snapshots to create and delete (default is 2): {}", numSnapshots);
  }

  @Override
  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    List<JobModel> createSnapshotJobResponses =
        IntStream.range(0, numSnapshots)
            .parallel()
            .mapToObj(
                i -> {
                  try {
                    JobModel createSnapshotJobResponse =
                        DataRepoUtils.createSnapshotWithoutWaiting(
                            repositoryApi, datasetSummaryModel, "snapshot-simple.json", true);
                    logger.info("Launched request to make snapshot {} of {}", i + 1, numSnapshots);
                    return createSnapshotJobResponse;
                  } catch (Exception ex) {
                    throw new RuntimeException("Failed to create snapshots in parallel", ex);
                  }
                })
            .collect(Collectors.toList());

    for (int i = 0; i < numSnapshots; i++) {
      JobModel createSnapshotJobResponse = createSnapshotJobResponses.get(i);
      JobModel createSnapshotJobResult =
          DataRepoUtils.waitForJobToFinish(
              repositoryApi, createSnapshotJobResponse, datasetCreator);
      SnapshotSummaryModel snapshotSummaryModel =
          DataRepoUtils.expectJobSuccess(
              repositoryApi, createSnapshotJobResult, SnapshotSummaryModel.class);
      snapshotList.add(snapshotSummaryModel);
      logger.info(
          "Successfully created snapshot: {}, index: {}", snapshotSummaryModel.getName(), i);
    }

    for (int i = 0; i < snapshotList.size(); i++) {
      SnapshotSummaryModel snapshotSummaryModel = snapshotList.get(i);
      // Now delete each snapshot
      JobModel deleteSnapshotJobResponse =
          repositoryApi.deleteSnapshot(snapshotSummaryModel.getId());
      JobModel deleteSnapshotJobResult =
          DataRepoUtils.waitForJobToFinish(
              repositoryApi, deleteSnapshotJobResponse, datasetCreator);
      //  ^ Shelby makes the great point that this wait is v artificial
      DataRepoUtils.expectJobSuccess(
          repositoryApi, deleteSnapshotJobResult, DeleteResponseModel.class);
      logger.info(
          "Successfully deleted snapshot: {}, index: {}", snapshotSummaryModel.getName(), i);
    }
  }

  @Override
  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    //  get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // Delete any snapshots that were not deleted by the test
    for (int i = 0; i < snapshotList.size(); i++) {
      SnapshotModel snapshot;
      try {
        snapshot =
            repositoryApi.retrieveSnapshot(snapshotList.get(i).getId(), Collections.emptyList());
      } catch (ApiException e) {
        snapshot = null;
        logger.info("Snapshot already deleted: {}", snapshotList.get(i).getName());
      }
      if (snapshot != null) {
        JobModel deleteSnapshotJobResponse =
            repositoryApi.deleteSnapshot(snapshotList.get(i).getId());
        deleteSnapshotJobResponse =
            DataRepoUtils.waitForJobToFinish(
                repositoryApi, deleteSnapshotJobResponse, datasetCreator);
        DataRepoUtils.expectJobSuccess(
            repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
        logger.info("Successfully cleaned up snapshot: {}", snapshotList.get(i).getName());
      }
    }
    // delete the scratch files used for ingesting tabular data and soft delete rows
    StorageUtils.deleteFiles(
        StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);

    // delete the profile and dataset
    super.cleanup(testUsers);
  }
}
