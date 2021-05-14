package bio.terra.service.resourcemanagement;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
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

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
    private Dataset dataset;

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(iamService);
        dataProjectPrefix = resourceConfiguration.getDataProjectPrefix();

        dataset = new Dataset();
        dataset.id(UUID.randomUUID());
        dataset.name("adataset");
    }

    @After
    public void tearDown() throws Exception {
        connectedOperations.teardown();
        resourceConfiguration.setDataProjectPrefix(dataProjectPrefix);
    }
    @Test
    public void shouldGetCorrectIdForDataset() {
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(dataset.getId(), billingProfile);
        String expectedProfileId =
            resourceConfiguration.getDataProjectPrefixToUse() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForDatasetWithSpecialChars() {
        String oddProfileName = "abc & 123";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile().profileName(oddProfileName);
        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(dataset.getId(), billingProfile);
        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse() + "-abc---123";
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForSnapshot() {
        UUID snapshotId = UUID.randomUUID();
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForSnapshot(snapshotId, billingProfile);
        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse() + "-" +
            billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForFile() {
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForFile(dataset, billingProfile);
        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse() + "-" +
            billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForBucket() {
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.bucketForFile(dataset, billingProfile);
        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse()
            + "-" + billingProfile.getProfileName()
            + "-bucket";
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForDatasetWithPrefix() {
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(dataset.getId(), billingProfile);
        String expectedProfileId =
            resourceConfiguration.getProjectId() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect before changing prefix", projectId, equalTo(expectedProfileId));
        resourceConfiguration.setDataProjectPrefix("PREFIX");
        String projectIdWithPrefix = oneProjectPerProfileIdSelector.projectIdForDataset(dataset.getId(),
            billingProfile);
        String expectedProfileIdWithPrefix =
            resourceConfiguration.getDataProjectPrefix() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect after changing prefix", projectIdWithPrefix,
            equalTo(expectedProfileIdWithPrefix));
    }
}
