package scripts.testscripts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DatasetPatchRequestModel;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyResponse;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.DataRepoUtils;

public class RetrieveDataset extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(RetrieveDataset.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public RetrieveDataset() {
    super();
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);
    DatasetModel datasetModel =
        repositoryApi.retrieveDataset(datasetSummaryModel.getId(), Collections.emptyList());
    logger.info(
        "Successfully retrieved dataset: name = {}, data project = {}",
        datasetModel.getName(),
        datasetModel.getDataProject());

    // add test user as steward so that they have access to edit phs id
    try {
      logger.debug("Adding test user {} as steward", testUser.userEmail);
      PolicyResponse policyResponse =
          repositoryApi.addDatasetPolicyMember(
              datasetSummaryModel.getId(),
              "steward",
              new PolicyMemberRequest().email(testUser.userEmail));
    } catch (ApiException e) {
      throw new RuntimeException("Error adding user as steward", e);
    }

    // Test editing PhsId via patch endpoint
    DatasetPatchRequestModel request = new DatasetPatchRequestModel();
    String newPhsId = "phs123456";
    request.setPhsId(newPhsId);

    // Retry patch dataset endpoint
    // Can run into concurrent update error
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                assertThat(
                    "PhsId has been updated for dataset",
                    repositoryApi.patchDataset(datasetSummaryModel.getId(), request).getPhsId(),
                    equalTo(newPhsId));
              } catch (Exception ex) {
                return false;
              }
              return true;
            });
  }
}
