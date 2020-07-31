package testscripts;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import testscripts.baseclasses.SimpleDataset;
import utils.DataRepoUtils;
import utils.SAMUtils;

public class DatasetCustodianPermissions extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(DatasetCustodianPermissions.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public DatasetCustodianPermissions() {
    super();
  }

  // TODO: also check BQ permissions before/after adding test user as a custodian
  public void userJourney(TestUserSpecification testUser) throws Exception {
    // check with SAM if the user is a steward
    org.broadinstitute.dsde.workbench.client.sam.ApiClient samClient =
        SAMUtils.getClientForTestUser(testUser, server);
    boolean isSteward = SAMUtils.isDataRepoSteward(samClient, server.samResourceIdForDatarepo);
    logger.info("testUser {} isSteward = {}", testUser.name, isSteward);

    // try #1 to retrieve the dataset
    ApiClient datarepoClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(datarepoClient);
    boolean retrieveDatasetIsUnauthorized =
        DataRepoUtils.retrieveDatasetIsUnauthorized(repositoryApi, datasetSummaryModel.getId());
    if (isSteward) {
      assertThat(
          "Test user " + testUser.name + " is a steward and can retrieve the dataset",
          !retrieveDatasetIsUnauthorized);
      return; // stewards already have permissions, so only check custodian/reader permissions for
      // non-stewards
    }

    assertThat(
        "Test user " + testUser.name + " is not a steward and cannot retrieve the dataset",
        retrieveDatasetIsUnauthorized);

    // add test user as a custodian
    ApiClient datasetCreatorDatarepoClient =
        DataRepoUtils.getClientForTestUser(datasetCreator, server);
    RepositoryApi datasetCreatorRepositoryApi = new RepositoryApi(datasetCreatorDatarepoClient);
    PolicyResponse policyResponse =
        datasetCreatorRepositoryApi.addDatasetPolicyMember(
            datasetSummaryModel.getId(),
            "custodian",
            new PolicyMemberRequest().email(testUser.userEmail));
    logger.info("policyresponse {}", policyResponse);

    // try #2 to retrieve the dataset
    retrieveDatasetIsUnauthorized =
        DataRepoUtils.retrieveDatasetIsUnauthorized(repositoryApi, datasetSummaryModel.getId());
    assertThat(
        "Test user " + testUser.name + " is a custodian and can retrieve the dataset",
        !retrieveDatasetIsUnauthorized);
  }
}
