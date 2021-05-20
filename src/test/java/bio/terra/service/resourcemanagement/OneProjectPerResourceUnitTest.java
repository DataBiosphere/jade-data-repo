package bio.terra.service.resourcemanagement;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
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
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class OneProjectPerResourceUnitTest {

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private GoogleResourceDao resourceDao;

    @Autowired
    private OneProjectPerResourceSelector oneProjectPerResourceSelector;

    @Autowired
    private GoogleResourceConfiguration resourceConfiguration;

    private List<BillingProfileModel> billingProfiles;
    private List<GoogleProjectResource> projects;
    private List<Dataset> datasets;
    private String dataProjectPrefix;

    @Before
    public void setup() throws IOException, InterruptedException {
        // Initialize lists
        billingProfiles = new ArrayList<>();
        projects = new ArrayList<>();
        datasets = new ArrayList<>();

        // Two billing profiles
        BillingProfileRequestModel profileRequest1 = ProfileFixtures.randomBillingProfileRequest();
        billingProfiles.add(profileDao.createBillingProfile(profileRequest1, "testUser"));

        BillingProfileRequestModel profileRequest2 = ProfileFixtures.randomBillingProfileRequest();
        billingProfiles.add(profileDao.createBillingProfile(profileRequest2, "testUser"));

        // two google project resources
        GoogleProjectResource projectResource1 = ResourceFixtures.randomProjectResource(billingProfiles.get(0));
        projectResource1.googleProjectId(oneProjectPerResourceSelector.projectIdForDataset());
        UUID projectResourceId1 = resourceDao.createProject(projectResource1);
        projectResource1.id(projectResourceId1);
        projectResource1.profileId(UUID.fromString(billingProfiles.get(0).getId()));
        projects.add(projectResource1);

        GoogleProjectResource projectResource2 = ResourceFixtures.randomProjectResource(billingProfiles.get(1));
        projectResource2.googleProjectId(oneProjectPerResourceSelector.projectIdForDataset());
        UUID projectResourceId2 = resourceDao.createProject(projectResource2);
        projectResource2.id(projectResourceId2);
        projectResource2.profileId(UUID.fromString(billingProfiles.get(1).getId()));
        projects.add(projectResource2);

        dataProjectPrefix = resourceConfiguration.getDataProjectPrefix();

    }

    @After
    public void teardown() {
        for (Dataset dataset : datasets) {
            datasetDao.delete(dataset.getId());
        }
        for (GoogleProjectResource project : projects) {
            resourceDao.deleteProject(project.getId());
        }
        for (BillingProfileModel billingProfile : billingProfiles) {
            profileDao.deleteBillingProfileById(UUID.fromString(billingProfile.getId()));
        }
        resourceConfiguration.setDataProjectPrefix(dataProjectPrefix);
    }

    @Test
    public void snapshotDatasetSeparateProjects() throws Exception {
        Dataset dataset = createDataset(billingProfiles.get(0), projects.get(0));
        String datasetProjectId = projects.get(0).getGoogleProjectId();
        datasets.add(dataset);


        // this will now always be random and different than the dataset project id
        String snapshotProjectId = oneProjectPerResourceSelector.projectIdForSnapshot();
        assertThat("Project ID for snapshot is not the same as the dataset's project id",
            snapshotProjectId, not(datasetProjectId));
    }

    @Test
    public void bucketPerDatasetPerBilling() throws Exception {
        Dataset dataset = createDataset(billingProfiles.get(0), projects.get(0));
        String datasetProjectId = projects.get(0).getGoogleProjectId();
        datasets.add(dataset);


        //Same billing profile as source dataset, so same project id
        String fileProjectId = oneProjectPerResourceSelector.projectIdForFile(dataset, billingProfiles.get(0));
        assertThat("For same billing, dataset and file project are the same",
            fileProjectId, equalTo(datasetProjectId));
        String bucketProjectId = oneProjectPerResourceSelector.bucketForFile(dataset, billingProfiles.get(0));
        assertThat("File project are the same, plus bucket suffix",
            bucketProjectId, equalTo(fileProjectId + "-bucket"));


        //Different billing profile than source dataset
        BillingProfileModel newBillingProfile = billingProfiles.get(0).id(UUID.randomUUID().toString());
        String expectedDiffFileProjectName = datasetProjectId + "-storage";

        String diffFileProjectId = oneProjectPerResourceSelector.projectIdForFile(dataset, newBillingProfile);
        assertThat(
            "For different billing, dataset and file project are the same with -storage suffix for file project",
            diffFileProjectId, equalTo(expectedDiffFileProjectName));

        String diffBucketProjectId = oneProjectPerResourceSelector.bucketForFile(dataset, newBillingProfile);
        assertThat("File project are the same, plus bucket suffix",
            diffBucketProjectId, equalTo(diffFileProjectId + "-bucket"));
    }

    @Test
    public void twoDatasetsTwoBuckets() throws Exception {
        //Two dataset, two billing profiles
        datasets.add(createDataset(billingProfiles.get(0), projects.get(0)));
        datasets.add(createDataset(billingProfiles.get(1), projects.get(1)));

        String bucketName1 = oneProjectPerResourceSelector.bucketForFile(
            datasets.get(0), billingProfiles.get(0));
        String bucketName2 = oneProjectPerResourceSelector.bucketForFile(
            datasets.get(1), billingProfiles.get(1));

        assertNotEquals("Buckets should be named differently", bucketName1, bucketName2);
    }

    @Test
    public void shouldGetCorrectIdForDatasetWithPrefix() {
        String projectId = oneProjectPerResourceSelector.projectIdForDataset();
        assertThat("Project ID is what we expect before changing prefix", projectId,
            startsWith(resourceConfiguration.getProjectId()));

        resourceConfiguration.setDataProjectPrefix("PREFIX");
        String projectIdWithPrefix = oneProjectPerResourceSelector.projectIdForDataset();
        assertThat("Project ID is starts with newly set prefix", projectIdWithPrefix,
            startsWith(resourceConfiguration.getDataProjectPrefix() ));
    }

    private Dataset createDataset(BillingProfileModel billingProfile, GoogleProjectResource project)
        throws IOException {
        Dataset dataset;
        DatasetRequestModel datasetRequest1 =
            jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
        datasetRequest1.name(datasetRequest1.getName() + UUID.randomUUID().toString())
            .defaultProfileId(billingProfile.getId());
        dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest1);
        dataset.projectResource(project);
        dataset.projectResourceId(project.getId());
        String createFlightId1 = UUID.randomUUID().toString();
        dataset.id(UUID.randomUUID());
        datasetDao.createAndLock(dataset, createFlightId1);
        datasetDao.unlockExclusive(dataset.getId(), createFlightId1);
        return dataset;
    }
}
