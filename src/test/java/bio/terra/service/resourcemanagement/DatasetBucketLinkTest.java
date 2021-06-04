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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetBucketLinkTest {

    private static final Logger logger = LoggerFactory.getLogger(DatasetBucketLinkTest.class);

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private GoogleResourceDao resourceDao;

    @Autowired
    private OneProjectPerProfileIdSelector oneProjectPerProfileIdSelector;

    private List<BillingProfileModel> billingProfiles;
    private List<GoogleProjectResource> projects;
    private List<Dataset> datasets;

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
        UUID projectId = resourceDao.createProject(projectResource1);
        projectResource1.id(projectId);
        projects.add(projectResource1);

        GoogleProjectResource projectResource2 = ResourceFixtures.randomProjectResource(billingProfiles.get(1));
        UUID projectId2 = resourceDao.createProject(projectResource2);
        projectResource2.id(projectId2);
        projects.add(projectResource2);

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
            profileDao.deleteBillingProfileById(billingProfile.getId());
        }
    }

    @Test
    public void twoDatasetsTwoBillingProfilesTwoBuckets() throws Exception {
        //Two dataset, two billing profiles
        datasets.add(createDataset(billingProfiles.get(0), projects.get(0).getId()));
        datasets.add(createDataset(billingProfiles.get(1), projects.get(1).getId()));

        String bucketName1 = oneProjectPerProfileIdSelector.bucketForFile(
            datasets.get(0).getName(), billingProfiles.get(0));
        String bucketName2 = oneProjectPerProfileIdSelector.bucketForFile(
            datasets.get(1).getName(), billingProfiles.get(1));
        logger.info("Bucket 1: {}; Bucket 2: {}", bucketName1, bucketName2);

        assertNotEquals("Buckets should be named differently", bucketName1, bucketName2);
    }

    @Test
    public void twoDatasetsOneBillingProfileOneBucket() throws Exception {

        //Two dataset, one billing profile
        datasets.add(createDataset(billingProfiles.get(0), projects.get(0).getId()));
        datasets.add(createDataset(billingProfiles.get(0), projects.get(0).getId()));

        String bucketName1 = oneProjectPerProfileIdSelector.bucketForFile(
            datasets.get(0).getName(), billingProfiles.get(0));
        String bucketName2 = oneProjectPerProfileIdSelector.bucketForFile(
            datasets.get(1).getName(), billingProfiles.get(0));
        logger.info("Bucket 1: {}; Bucket 2: {}", bucketName1, bucketName2);

        assertEquals("Buckets should be named the same", bucketName1, bucketName2);
    }

    private Dataset createDataset(BillingProfileModel billingProfile, UUID projectId) throws IOException {
        Dataset dataset;
        DatasetRequestModel datasetRequest1 = jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
        datasetRequest1.name(datasetRequest1.getName() + UUID.randomUUID().toString())
            .defaultProfileId(billingProfile.getId());
        dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest1);
        dataset.projectResourceId(projectId);
        String createFlightId1 = UUID.randomUUID().toString();
        dataset.id(UUID.randomUUID());
        datasetDao.createAndLock(dataset, createFlightId1);
        datasetDao.unlockExclusive(dataset.getId(), createFlightId1);
        return dataset;
    }
}
