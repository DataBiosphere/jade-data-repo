package scripts.testscripts.baseclasses;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.api.ResourcesApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.CloudPlatform;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.DeleteResponseModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyResponse;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.utils.DataRepoUtils;
import scripts.utils.SAMUtils;

public class SimpleDataset extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(SimpleDataset.class);

  protected TestUserSpecification datasetCreator;
  protected BillingProfileModel billingProfileModel;
  protected DatasetSummaryModel datasetSummaryModel;
  protected boolean deleteProfile = true;

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // pick the a user that is a Data Repo steward to be the dataset creator if it hasn't already
    // been set
    if (datasetCreator == null) {
      datasetCreator = SAMUtils.findTestUserThatIsDataRepoSteward(testUsers, server);
      if (datasetCreator == null) {
        throw new IllegalArgumentException("None of the test users are Data Repo stewards.");
      }
    } else {
      logger.info("datasetCreator was set before base class initialization.");
    }
    logger.info("datasetCreator: {}", datasetCreator.name);

    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    ResourcesApi resourcesApi = new ResourcesApi(datasetCreatorClient);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // create a new profile
    if (billingProfileModel == null) {
      if (cloudPlatform.equals(CloudPlatform.GCP)) {
        billingProfileModel =
            DataRepoUtils.createProfile(
                resourcesApi,
                repositoryApi,
                billingAccount,
                "profile-simple",
                datasetCreator,
                true);
      } else if (cloudPlatform.equals(CloudPlatform.AZURE)) {
        billingProfileModel =
            DataRepoUtils.createAzureProfile(
                resourcesApi,
                repositoryApi,
                tenantId,
                subscriptionId,
                resourceGroupName,
                applicationDeploymentName,
                "azure-profile-simple",
                billingAccount,
                datasetCreator,
                true);
      } else {
        throw new RuntimeException("Unsupported cloud platform");
      }
      logger.info("Successfully created profile: {}", billingProfileModel.getProfileName());
    } else {
      logger.info("Using existing profile: {}", billingProfileModel.getProfileName());
    }

    // make the create dataset request and wait for the job to finish
    JobModel createDatasetJobResponse =
        DataRepoUtils.createDataset(
            repositoryApi,
            billingProfileModel.getId(),
            cloudPlatform,
            "dataset-simple.json",
            datasetCreator,
            true,
            false);

    // save a reference to the dataset summary model so we can delete it in cleanup()
    datasetSummaryModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);
    logger.info("Successfully created dataset: {}", datasetSummaryModel.getName());

    // add users as custodians
    List<TestUserSpecification> custodians =
        testUsers.stream()
            .filter(u -> !StringUtils.equals(u.userEmail, datasetCreator.userEmail))
            .collect(Collectors.toList());

    custodians.forEach(
        u -> {
          try {
            logger.info("Adding user {} as custodian", u.userEmail);
            PolicyResponse policyResponse =
                repositoryApi.addDatasetPolicyMember(
                    datasetSummaryModel.getId(),
                    "custodian",
                    new PolicyMemberRequest().email(u.userEmail));
          } catch (ApiException e) {
            throw new RuntimeException("Error adding user as custodian", e);
          }
        });
  }

  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {
    // get the ApiClient for the dataset creator
    ApiClient datasetCreatorClient = DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

    // make the delete dataset request and wait for the job to finish
    JobModel deleteDatasetJobResponse = repositoryApi.deleteDataset(datasetSummaryModel.getId());
    deleteDatasetJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, deleteDatasetJobResponse, datasetCreator);
    DataRepoUtils.expectJobSuccess(
        repositoryApi, deleteDatasetJobResponse, DeleteResponseModel.class);
    logger.info("Successfully deleted dataset: {}", datasetSummaryModel.getName());

    // delete the profile
    if (deleteProfile) {
      TestUserSpecification admin = SAMUtils.findTestUserThatIsDataRepoAdmin(testUsers, server);
      ApiClient adminClient = DataRepoUtils.getClientForTestUser(admin, server);
      ResourcesApi adminResourcesApi = new ResourcesApi(adminClient);
      adminResourcesApi.deleteProfile(billingProfileModel.getId(), true);
      logger.info("Successfully deleted profile: {}", billingProfileModel.getProfileName());
    } else {
      logger.info("Skipping profile delete because test is using a shared profile");
    }
  }
}
