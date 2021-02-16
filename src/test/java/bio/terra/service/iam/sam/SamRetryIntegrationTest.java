package bio.terra.service.iam.sam;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.iam.IamResourceType;
import bio.terra.model.PolicyModel;
import org.junit.After;
import org.junit.Before;
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

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class SamRetryIntegrationTest extends UsersBase {
    private static final Logger logger = LoggerFactory.getLogger(SamRetryIntegrationTest.class);
    @Autowired
    private AuthService authService;
    @Autowired
    private DataRepoFixtures dataRepoFixtures;
    @Autowired
    private IamProviderInterface iam;

    private String stewardToken;
    private UUID fakeDatasetId;
    private AuthenticatedUserRequest userRequest;

    @Before
    public void setup() throws Exception {
        super.setup();
        stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
        dataRepoFixtures.resetConfig(steward());
        userRequest = new AuthenticatedUserRequest()
            .email(steward().getEmail())
            .token(java.util.Optional.ofNullable(stewardToken));
        fakeDatasetId = UUID.randomUUID();
    }

    @After
    public void teardown() throws Exception {
        dataRepoFixtures.resetConfig(steward());

        iam.deleteDatasetResource(userRequest, fakeDatasetId);
    }

    @Test
    public void retrySyncDatasetPolicies() throws InterruptedException {
        iam.createDatasetResource(userRequest, fakeDatasetId);

        iam.syncDatasetResourcePolicies(userRequest, fakeDatasetId);
        List<PolicyModel> firstSync_policyList = iam.retrievePolicies(userRequest, IamResourceType.DATASET, fakeDatasetId);

        // Should be able to re-run syncDatasetResourcePolicies without an error being thrown
        // Otherwise, need to break up each "SamIam.syncOnePolicy" into own retry loop
        iam.syncDatasetResourcePolicies(userRequest, fakeDatasetId);
        List<PolicyModel> secondSync_policyList = iam.retrievePolicies(userRequest, IamResourceType.DATASET, fakeDatasetId);

        // Let's make sure the policy model didn't change between the first and second sync
        for(int i = 0; i < firstSync_policyList.size(); i++){
            PolicyModel firstSync_policy = firstSync_policyList.get(i);
            PolicyModel secondSync_policy = secondSync_policyList.get(i);

            logger.info("[TEST INFO] Checking Role {} - {} of {} SAM policies synced",
                firstSync_policy.getName(), (i + 1), firstSync_policyList.size());
            assertTrue("Policy should not have changed after second sync",
                firstSync_policy.equals(secondSync_policy));

        }
    }

}
