package bio.terra.service.resourcemanagement;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
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

@ActiveProfiles({"test", "google"})
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Connected.class)
public class OneProjectPerDatasetIdSelectorTest {
    @Autowired
    private GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    private OneProjectPerDatasetIdSelector oneProjectPerDatasetIdSelector;

    @Autowired
    private ConnectedOperations connectedOperations;

    @MockBean
    private IamProviderInterface iamService;

    private String dataProjectPrefix;
    private Dataset dataset;

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private GoogleResourceDao resourceDao;

    private BillingProfileModel billingProfile;
    private UUID projectId;
    private String snapshotName;
    private GoogleProjectResource projectResource;
    private String expectedProjectName;

    private UUID createDataset(DatasetRequestModel datasetRequest, String newName) throws Exception {
        datasetRequest.name(newName).defaultProfileId(billingProfile.getId());
        dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
        dataset.projectResourceId(projectId);
        String createFlightId = UUID.randomUUID().toString();
        UUID datasetId = UUID.randomUUID();
        dataset.id(datasetId);
        datasetDao.createAndLock(dataset, createFlightId);
        datasetDao.unlockExclusive(dataset.getId(), createFlightId);
        return datasetId;
    }

    private UUID createDataset(String datasetFile) throws Exception  {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
        return createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID().toString());
    }

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(iamService);
        dataProjectPrefix = resourceConfiguration.getDataProjectPrefix();

        BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
        billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

        projectResource = ResourceFixtures.randomProjectResource(billingProfile);
        projectId = resourceDao.createProject(projectResource);
        projectResource.id(projectId);

        createDataset("dataset-minimal.json");
        snapshotName = "asnapshot";
        expectedProjectName =
            resourceConfiguration.getDataProjectPrefixToUse() + "-" + dataset.getId();

    }

    @After
    public void tearDown() throws Exception {
        //TODO - Add cleanup
        connectedOperations.teardown();
        resourceConfiguration.setDataProjectPrefix(dataProjectPrefix);
    }
    @Test
    public void shouldGetCorrectIdForDataset() {
        String projectId = oneProjectPerDatasetIdSelector.projectIdForDataset(dataset, billingProfile);
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProjectName));
    }

    @Test
    public void shouldGetCorrectIdForSnapshot() {
        String projectId = oneProjectPerDatasetIdSelector.projectIdForSnapshot(snapshotName, dataset, billingProfile);
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProjectName));
    }

//    @Test
//    public void shouldGetCorrectIdForFile() {
//        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
//        String projectId = oneProjectPerDatasetIdSelector.projectIdForFile(dataset, billingProfile);
//        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse() + "-" +
//            billingProfile.getProfileName();
//        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
//    }
//
//    @Test
//    public void shouldGetCorrectIdForBucket() {
//        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
//        String projectId = oneProjectPerDatasetIdSelector.bucketForFile(dataset, billingProfile);
//        String expectedProfileId = resourceConfiguration.getDataProjectPrefixToUse()
//            + "-" + billingProfile.getProfileName()
//            + "-bucket";
//        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
//    }
//
//    @Test
//    public void shouldGetCorrectIdForDatasetWithPrefix() {
//        BillingProfileModel billingProfile = ProfileFixtures.randomBillingProfile();
//        String projectId = oneProjectPerDatasetIdSelector.projectIdForDataset(dataset, billingProfile);
//        String expectedProfileId =
//            resourceConfiguration.getProjectId() + "-" + billingProfile.getProfileName();
//        assertThat("Project ID is what we expect before changing prefix", projectId, equalTo(expectedProfileId));
//        resourceConfiguration.setDataProjectPrefix("PREFIX");
//        String projectIdWithPrefix = oneProjectPerDatasetIdSelector.projectIdForDataset(dataset, billingProfile);
//        String expectedProfileIdWithPrefix =
//            resourceConfiguration.getDataProjectPrefix() + "-" + billingProfile.getProfileName();
//        assertThat("Project ID is what we expect after changing prefix", projectIdWithPrefix,
//            equalTo(expectedProfileIdWithPrefix));
//    }
}
