package bio.terra.service.auth.iam.sam;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.SamPolicyModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class SamRetryIntegrationTest extends UsersBase {
  private static final Logger logger = LoggerFactory.getLogger(SamRetryIntegrationTest.class);
  @Autowired private AuthService authService;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private IamProviderInterface iam;
  @Autowired private SamConfiguration samConfig;
  @Rule @Autowired public TestJobWatcher testWatcher;

  private String stewardToken;
  private UUID fakeDatasetId;
  private AuthenticatedUserRequest userRequest;
  private GoogleApi samGoogleApi;

  private ApiClient getApiClient(String accessToken) {
    ApiClient apiClient = new ApiClient();
    apiClient.setAccessToken(accessToken);
    apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java"); // only logs an error in sam
    return apiClient.setBasePath(samConfig.getBasePath());
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    dataRepoFixtures.resetConfig(steward());
    userRequest =
        AuthenticatedUserRequest.builder()
            .setSubjectId("SamIntegration")
            .setEmail(steward().getEmail())
            .setToken(stewardToken)
            .build();
    fakeDatasetId = UUID.randomUUID();
    samGoogleApi = new GoogleApi(getApiClient(stewardToken));
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    iam.deleteDatasetResource(userRequest, fakeDatasetId);
  }

  @Test
  public void retrySyncDatasetPolicies() throws InterruptedException, ApiException {
    iam.createDatasetResource(userRequest, fakeDatasetId);

    // Should be able to re-run syncDatasetResourcePolicies without an error being thrown
    // Otherwise, need to break up each "SamIam.syncOnePolicy" into own retry loop
    String policyEmail = SyncPolicy(IamResourceType.DATASET, fakeDatasetId, IamRole.STEWARD);
    logger.info("[TEST INFO] Policy email on first sync: {}", policyEmail);
    List<SamPolicyModel> firstSync_policyList =
        iam.retrievePolicies(userRequest, IamResourceType.DATASET, fakeDatasetId);

    String second_policyEmail = SyncPolicy(IamResourceType.DATASET, fakeDatasetId, IamRole.STEWARD);
    logger.info("[TEST INFO] Policy email on second sync: {}", policyEmail);
    List<SamPolicyModel> secondSync_policyList =
        iam.retrievePolicies(userRequest, IamResourceType.DATASET, fakeDatasetId);

    assertEquals("Policy Emails should be the same", policyEmail, second_policyEmail);

    // Let's make sure the policy model didn't change between the first and second sync
    for (int i = 0; i < firstSync_policyList.size(); i++) {
      SamPolicyModel firstSync_policy = firstSync_policyList.get(i);
      SamPolicyModel secondSync_policy = secondSync_policyList.get(i);

      logger.info(
          "[TEST INFO] Checking Role {} - {} of {} SAM policies synced",
          firstSync_policy.getName(),
          (i + 1),
          firstSync_policyList.size());
      assertTrue(
          "Policy should not have changed after second sync",
          firstSync_policy.equals(secondSync_policy));
    }
  }

  private String SyncPolicy(IamResourceType resourceType, UUID resourceId, IamRole role)
      throws ApiException {

    Map<String, List<Object>> results =
        samGoogleApi.syncPolicy(resourceType.toString(), resourceId.toString(), role.toString());
    return results.keySet().iterator().next();
  }
}
