package bio.terra.service.dataset;

import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.Table;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ResourceService resourceService;

    private BillingProfileModel billingProfile;
    private GoogleBucketResource bucketForFile;
    private UUID projectId;
    private Dataset dataset;

    private UUID datasetId;
    private UUID bucketResourceId;

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

    private UUID createBucket() throws InterruptedException {
        String ingestFileFlightId = UUID.randomUUID().toString();
        bucketForFile =
            resourceService.getOrCreateBucketForFile(
                dataset.getName(),
                billingProfile,
                ingestFileFlightId);
        return bucketForFile.getResourceId();
    }

    private UUID createDataset(String datasetFile) throws Exception  {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
        return createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID().toString());
    }

    @Before
    public void setup() {
        logger.info("-------------------Setup----------------------");
        BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
        billingProfile = profileDao.createBillingProfile(profileRequest, "hi@hi.hi");

        GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
        projectId = resourceDao.createProject(projectResource);
        logger.info("-------------------Test----------------------");
    }

    @After
    public void teardown() {
        logger.info("-------------------Cleanup----------------------");
        datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
        datasetDao.delete(datasetId);
        resourceDao.deleteProject(projectId);
        profileDao.deleteBillingProfileById(UUID.fromString(billingProfile.getId()));
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

        // create link
        datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertTrue("Link should now exist.", linkExists);

        // create link
        datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertTrue("Link should now exist.", linkExists);

        // delete link
        datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertFalse("Link should no longer exists.", linkExists);
    }

    // TODO: Fix code to allow this test to pass or change test to match expected behavior
    @Ignore
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
        // Is this the expected behavior?
        // Since we're returning COUNT(*) in the exists check, even a successful_ingests=0 will return 1
        datasetBucketDao.decrementDatasetBucketLink(datasetId, bucketResourceId);
        linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
        assertFalse("After decrementing bucket link, Link should no longer exist.", linkExists);
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
        UUID randomBucketResourceId= UUID.randomUUID();

        //initial check - link should not yet exist
        boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, randomBucketResourceId);
        assertFalse("Link should not yet exist.", linkExists);

        // this should fail -> no requires real dataset and bucket to link
        datasetBucketDao.createDatasetBucketLink(datasetId, randomBucketResourceId);
    }
}
