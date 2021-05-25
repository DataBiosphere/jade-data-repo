package bio.terra.service.resourcemanagement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@ActiveProfiles({"terra", "google"})
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Connected.class)
public class OneProjectPerProfileIdSelectorTest {
    @Autowired
    private GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    private OneProjectPerProfileIdSelector oneProjectPerProfileIdSelector;

    @Autowired
    private ConnectedOperations connectedOperations;

    @MockBean
    private IamProviderInterface iamService;

    private String dataProjectPrefix;

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(iamService);
        dataProjectPrefix = resourceConfiguration.getDataProjectPrefix();
    }

    @After
    public void tearDown() throws Exception {
        connectedOperations.teardown();
        resourceConfiguration.setDataProjectPrefix(dataProjectPrefix);
    }
    @Test
    public void shouldGetCorrectIdForDataset() throws Exception {
        String datasetName = "adataset";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(datasetName, billingProfile);
        String expectedProfileId =
            resourceConfiguration.getDataProjectPrefixToUse() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForDatasetWithSpecialChars() throws Exception {
        String datasetName = "adataset";
        String oddProfileName = "abc & 123";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile().profileName(oddProfileName);
        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(datasetName, billingProfile);
        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse() + "-abc---123";
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForSnapshot() throws Exception {
        String snapshotName = "asnapshot";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForSnapshot(snapshotName, billingProfile);
        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse() + "-" +
            billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForFile() throws Exception {
        String datasetName = "adataset";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForFile(datasetName, billingProfile);
        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse() + "-" +
            billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForBucket() throws Exception {
        UUID datasetId = UUID.randomUUID();
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.bucketForFile(datasetId, billingProfile);
        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse()
            + "-" + billingProfile.getProfileName()
            + "-bucket";
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForDatasetWithPrefix() throws Exception {
        String datasetName = "adataset";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(datasetName, billingProfile);
        String expectedProfileId =
            resourceConfiguration.getProjectId() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect before changing prefix", projectId, equalTo(expectedProfileId));
        resourceConfiguration.setDataProjectPrefix("PREFIX");
        String projectIdWithPrefix = oneProjectPerProfileIdSelector.projectIdForDataset(datasetName, billingProfile);
        String expectedProfileIdWithPrefix =
            resourceConfiguration.getDataProjectPrefix() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect after changing prefix", projectIdWithPrefix,
            equalTo(expectedProfileIdWithPrefix));
    }
}
