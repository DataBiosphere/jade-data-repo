package bio.terra.service.auth.iam.sam;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.IntegrationTestConfiguration;
import bio.terra.integration.Users;
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
import org.broadinstitute.dsde.workbench.client.sam.model.SyncReportEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
class SamRetryIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(SamRetryIntegrationTest.class);
  @Autowired private AuthService authService;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private IamProviderInterface iam;
  @Autowired private SamConfiguration samConfig;
  @Autowired private Users users;

  private TestConfiguration.User steward;
  private UUID fakeDatasetId;
  private AuthenticatedUserRequest userRequest;
  private GoogleApi samGoogleApi;

  private ApiClient getApiClient(String accessToken) {
    ApiClient apiClient = new ApiClient();
    apiClient.setAccessToken(accessToken);
    apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java"); // only logs an error in sam
    return apiClient.setBasePath(samConfig.basePath());
  }

  @BeforeEach
  public void setup() throws Exception {
    steward = users.steward();
    String stewardToken = authService.getDirectAccessAuthToken(steward.email());
    dataRepoFixtures.resetConfig(steward);
    userRequest =
        AuthenticatedUserRequest.builder()
            .setSubjectId("SamIntegration")
            .setEmail(steward.email())
            .setToken(stewardToken)
            .build();
    fakeDatasetId = UUID.randomUUID();
    samGoogleApi = new GoogleApi(getApiClient(stewardToken));
  }

  @AfterEach
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward);

    iam.deleteDatasetResource(userRequest, fakeDatasetId);
  }

  @Test
  void retrySyncDatasetPolicies() throws InterruptedException, ApiException {
    iam.createDatasetResource(userRequest, fakeDatasetId, null);

    // Should be able to re-run syncDatasetResourcePolicies without an error being thrown
    // Otherwise, need to break up each "SamIam.syncOnePolicy" into own retry loop
    String policyEmail = SyncPolicy(IamResourceType.DATASET, fakeDatasetId, IamRole.STEWARD);
    logger.info("[TEST INFO] Policy email on first sync: {}", policyEmail);
    List<SamPolicyModel> firstSyncPolicyList =
        iam.retrievePolicies(userRequest, IamResourceType.DATASET, fakeDatasetId);

    String secondPolicyEmail = SyncPolicy(IamResourceType.DATASET, fakeDatasetId, IamRole.STEWARD);
    logger.info("[TEST INFO] Policy email on second sync: {}", policyEmail);
    List<SamPolicyModel> secondSyncPolicyList =
        iam.retrievePolicies(userRequest, IamResourceType.DATASET, fakeDatasetId);

    assertEquals(policyEmail, secondPolicyEmail, "Policy Emails should be the same");

    // Let's make sure the policy model didn't change between the first and second sync
    for (int i = 0; i < firstSyncPolicyList.size(); i++) {
      SamPolicyModel firstSyncPolicy = firstSyncPolicyList.get(i);
      SamPolicyModel secondSyncPolicy = secondSyncPolicyList.get(i);

      logger.info(
          "[TEST INFO] Checking Role {} - {} of {} SAM policies synced",
          firstSyncPolicy.getName(),
          (i + 1),
          firstSyncPolicyList.size());
      assertEquals(
          firstSyncPolicy, secondSyncPolicy, "Policy should not have changed after second sync");
    }
  }

  private String SyncPolicy(IamResourceType resourceType, UUID resourceId, IamRole role)
      throws ApiException {

    Map<String, List<SyncReportEntry>> results =
        samGoogleApi.syncPolicy(
            resourceType.toString(), resourceId.toString(), role.toString(), null);
    return results.keySet().iterator().next();
  }
}
