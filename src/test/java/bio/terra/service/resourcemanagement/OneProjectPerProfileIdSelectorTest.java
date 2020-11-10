package bio.terra.service.resourcemanagement;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ActiveProfiles({"terra", "google"})
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Connected.class)
@Ignore
public class OneProjectPerProfileIdSelectorTest {
    @Autowired
    private GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    private OneProjectPerProfileIdSelector oneProjectPerProfileIdSelector;

    @Autowired
    private ConnectedOperations connectedOperations;

    @MockBean
    private IamProviderInterface iamService;

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(iamService);
    }

    @After
    public void tearDown() throws Exception {
        connectedOperations.teardown();
    }
    @Test
    public void shouldGetCorrectIdForDataset() throws Exception {
        String datasetName = "adataset";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(datasetName, billingProfile);
        String expectedProfileId = resourceConfiguration.getProjectId() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForDatasetWithSpecialChars() throws Exception {
        String datasetName = "adataset";
        String oddProfileName = "abc & 123";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile().profileName(oddProfileName);
        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(datasetName, billingProfile);
        String expectedProfileId = resourceConfiguration.getProjectId() + "-abc---123";
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForSnapshot() throws Exception {
        String snapshotName = "asnapshot";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForSnapshot(snapshotName, billingProfile);
        String expectedProfileId = resourceConfiguration.getProjectId() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForFile() throws Exception {
        String datasetName = "adataset";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.projectIdForFile(datasetName, billingProfile);
        String expectedProfileId = resourceConfiguration.getProjectId() + "-" + billingProfile.getProfileName();
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForBucket() throws Exception {
        String datasetName = "adataset";
        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
        String projectId = oneProjectPerProfileIdSelector.bucketForFile(datasetName, billingProfile);
        String expectedProfileId = resourceConfiguration.getProjectId()
            + "-" + billingProfile.getProfileName()
            + "-bucket";
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }
}
