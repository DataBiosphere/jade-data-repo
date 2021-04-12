package scripts.testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import com.google.cloud.storage.BlobId;
import common.utils.StorageUtils;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.DataRepoUtils;

public class SnapshotScaleDelete extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotScaleDelete.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public SnapshotScaleDelete() {
    super();
  }

  private SnapshotModel snapshotModel;

  private static List<BlobId> scratchFiles = new ArrayList<>();

  private int NUM_SNAPSHOTS = 1;

  public void setParameters(List<String> parameters) {
    if (parameters != null && parameters.size() > 0) {
      NUM_SNAPSHOTS = Integer.parseInt(parameters.get(0));
    }
    logger.debug("Number of snapshots to create and delete (default is 1): {}", NUM_SNAPSHOTS);
  }

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // create the profile and dataset
    super.setup(testUsers);
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    // DataRepositoryServiceApi dataRepositoryServiceApi = new DataRepositoryServiceApi(apiClient);
    List<SnapshotSummaryModel> snapshotList = new ArrayList<>();
    List<JobModel> deleteList = new ArrayList<>();
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      // make the create snapshot request and wait for the job to finish
      // the name of the snapshot will already be randomized based on the bool
      JobModel createSnapshotJobResponse =
          DataRepoUtils.createSnapshot(
              repositoryApi, datasetSummaryModel, "snapshot-simple.json", true);

      // save a reference to the snapshot summary model so we can delete it in cleanup(
      SnapshotSummaryModel snapshotSummaryModel =
          DataRepoUtils.expectJobSuccess(
              repositoryApi, createSnapshotJobResponse, SnapshotSummaryModel.class);
      snapshotList.add(snapshotSummaryModel);
      logger.info(
          "Successfully created snapshot: {}, index: {}", snapshotSummaryModel.getName(), i);
    }

    for (int i = 0; i < snapshotList.size(); i++) {
      // Now delete each snapshot
      JobModel deleteSnapshotJobResponse =
          repositoryApi.deleteSnapshot(snapshotList.get(i).getId());
      deleteList.add(deleteSnapshotJobResponse);
    }

    for (int i = 0; i < deleteList.size(); i++) {
      // Now check the delete of each snapshot
      JobModel deleteSnapshotJobResponse =
          DataRepoUtils.waitForJobToFinish(repositoryApi, deleteList.get(i));
      //  ^ Shelby makes the great point that this wait is v artificial
      DataRepoUtils.expectJobSuccess(
          repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
      logger.info("Successfully deleted snapshot: {}, index: {}", snapshotList.get(i).getName(), i);
    }
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete request and wait for the job to finish
    JobModel deleteSnapshotJobResponse = repositoryApi.deleteSnapshot(snapshotModel.getId());
    deleteSnapshotJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteSnapshotJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteSnapshotJobResponse, DeleteResponseModel.class);
    logger.info("Successfully deleted snapshot: {}", snapshotModel.getName());

    // delete the profile and dataset
    super.cleanup(testUsers);

    // delete the scratch files used for ingesting tabular data and soft delete rows
    StorageUtils.deleteFiles(
        StorageUtils.getClientForServiceAccount(server.testRunnerServiceAccount), scratchFiles);
  }
}
