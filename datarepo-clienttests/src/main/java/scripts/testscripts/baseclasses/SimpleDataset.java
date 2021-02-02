package scripts.testscripts.baseclasses;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.utils.DataRepoUtils;
import scripts.utils.SAMUtils;

import java.util.List;

public class SimpleDataset extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(SimpleDataset.class);

  protected TestUserSpecification datasetCreator;
  protected BillingProfileModel billingProfileModel;
  protected DatasetSummaryModel datasetSummaryModel;

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    setup(testUsers, null);
  }

  public void setup(List<TestUserSpecification> testUsers, BillingProfileModel billingProfile)
      throws Exception {
    // pick the a user that is a Data Repo steward to be the dataset creator
    datasetCreator = SAMUtils.findTestUserThatIsDataRepoSteward(testUsers, server);
    if (datasetCreator == null) {
      throw new IllegalArgumentException("None of the test users are Data Repo stewards.");
    }
    logger.debug("datasetCreator: {}", datasetCreator.name);

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // create a new profile
    if (billingProfile == null) {
      billingProfileModel =
          DataRepoUtils.createProfile(
              resourcesApi, repositoryApi, billingAccount, "profile-simple", true);
      logger.info("Successfully created profile: {}", billingProfileModel.getProfileName());
    } else {
      billingProfileModel = billingProfile;
      logger.info("Using existing profile: {}", billingProfileModel.getProfileName());
    }

    // make the create dataset request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(
            repositoryApi, billingProfileModel.getId(), "dataset-simple.json", true);

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);
    logger.info("Successfully created dataset: {}", datasetSummaryModel.getName());
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    cleanup(testUsers, true);
  }

  public void cleanup(List<TestUserSpecification> testUsers, boolean deleteProfile)
      throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete dataset request and wait for the job to finish
    JobModel deleteDatasetJobResponse = repositoryApi.deleteDataset(datasetSummaryModel.getId());
    deleteDatasetJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteDatasetJobResponse);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteDatasetJobResponse, DeleteResponseModel.class);
    logger.info("Successfully deleted dataset: {}", datasetSummaryModel.getName());

    // delete the profile
    if (deleteProfile) {
      resourcesApi.deleteProfile(billingProfileModel.getId());
      logger.info("Successfully deleted profile: {}", billingProfileModel.getProfileName());
    } else {
      logger.info("Skipping profile delete because test is using a shared profile");
    }
  }
}
