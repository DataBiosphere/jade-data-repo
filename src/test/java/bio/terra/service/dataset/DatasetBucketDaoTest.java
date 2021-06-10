package bio.terra.service.dataset;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.BucketResourceUtils;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
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
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetBucketDaoTest {
    private static final Logger logger = LoggerFactory.getLogger(DatasetBucketDaoTest.class);

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private DatasetBucketDao datasetBucketDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private GoogleResourceDao resourceDao;

    @Autowired
    private ConfigurationService configurationService;

    @Autowired
    private GoogleBucketService googleBucketService;

    @Autowired
    private GcsConfiguration gcsConfiguration;


    private BucketResourceUtils bucketResourceUtils = new BucketResourceUtils();
    private BillingProfileModel billingProfile;
    private GoogleBucketResource bucketForFile;
    private UUID projectId;
    private GoogleProjectResource projectResource;
    private Dataset dataset;

    private UUID datasetId;
    private UUID bucketResourceId;

    @Before
    public void setup() {
        bucketResourceUtils.setAllowReuseExistingBuckets(configurationService, true);
        BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
        billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

        projectResource = ResourceFixtures.randomProjectResource(billingProfile);
        projectId = resourceDao.createProject(projectResource);
        projectResource.id(projectId);
    }

    @After
    public void teardown() {
        try {
            datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
        } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to delete dataset  bucket link {}", bucketResourceId);
        }
        try {
            datasetDao.delete(datasetId);
        } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to delete dataset {}", datasetId);
        }
        try {
            resourceDao.deleteProject(projectId);
        } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to delete entry in database for project {}", projectId);
        }
        try {
            profileDao.deleteBillingProfileById(UUID.fromString(billingProfile.getId()));
        } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to billing profile {}", billingProfile.getId());
        }
        bucketResourceUtils.setAllowReuseExistingBuckets(configurationService, false);
    }

    @Test
    public void TestDatasetBucketLink() throws Exception {
        datasetId = createDataset("dataset-minimal.json");
        bucketResourceId = createBucket();

        //initial check - link should not yet exist
        boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertFalse("Link should not yet exist.", linkExists);

        // create link
        datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertTrue("Link should now exist.", linkExists);

        // delete link
        datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertFalse("Link should no longer exists.", linkExists);
    }

    @Test
    public void TestMultipleLinks() throws Exception {
        datasetId = createDataset("dataset-minimal.json");
        bucketResourceId = createBucket();

        //initial check - link should not yet exist
        boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertFalse("Link should not yet exist.", linkExists);

        // create link
        datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertTrue("Link should now exist.", linkExists);
        int linkCount = datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
        assertEquals("Link count should be 1.", 1, linkCount);

        // create link
        datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertTrue("Link should now exist.", linkExists);
        linkCount = datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
        assertEquals("Link count should be 2.", 2, linkCount);

        // delete link
        datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertFalse("Link should no longer exists.", linkExists);
    }

    @Test
    public void TestDecrementLink() throws Exception {
        datasetId = createDataset("dataset-minimal.json");
        bucketResourceId = createBucket();

        //initial check - link should not yet exist
        boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertFalse("Link should not yet exist.", linkExists);

        // create link
        datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertTrue("Link should now exist.", linkExists);

        // decrement the link
        datasetBucketDao.decrementDatasetBucketLink(datasetId, bucketResourceId);
        int linkCount = datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
        assertEquals("After decrementing bucket link, successful ingest count should equal 0.", 0, linkCount);
    }

    // Test key restraints - There must be entries in the dataset table and bucket_resource table
    // in order to create a link in the dataset_bucket table
    @Test(expected = Exception.class)
    public void DatasetMustExistToLink() throws Exception {

        // create dataset to pass to bucket create
        datasetId = createDataset("dataset-minimal.json");
        bucketResourceId = createBucket();

        // fake datasetId
        UUID randomDatasetId = UUID.randomUUID();

        //initial check - link should not yet exist
        boolean linkExists = datasetBucketDao.datasetBucketLinkExists(randomDatasetId, bucketResourceId);
        assertFalse("Link should not yet exist.", linkExists);

        // this should fail -> no requires real dataset and bucket to link
        datasetBucketDao.createDatasetBucketLink(randomDatasetId, bucketResourceId);
    }

    @Test(expected = Exception.class)
    public void BucketMustExistToLink() throws Exception {

        // create dataset
        datasetId = createDataset("dataset-minimal.json");

        // fake datasetId
        UUID randomBucketResourceId = UUID.randomUUID();

        //initial check - link should not yet exist
        boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, randomBucketResourceId);
        assertFalse("Link should not yet exist.", linkExists);

        // this should fail -> no requires real dataset and bucket to link
        datasetBucketDao.createDatasetBucketLink(datasetId, randomBucketResourceId);
    }


    private UUID createDataset(String datasetFile) throws Exception {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
        String newName = datasetRequest.getName() + UUID.randomUUID().toString();
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

    private UUID createBucket() throws InterruptedException {
        String ingestFileFlightId = UUID.randomUUID().toString();
        bucketForFile = googleBucketService.getOrCreateBucket(
            gcsConfiguration.getBucket(),
            projectResource,
            (GoogleRegion) dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BUCKET),
            ingestFileFlightId);
        return bucketForFile.getResourceId();
    }
}
