package bio.terra.service.resourcemanagement.google;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.load.LoadDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.BucketResourceLockTester;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.google.api.client.util.Lists;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class BucketResourceTest {
    private final Logger logger = LoggerFactory.getLogger(BucketResourceTest.class);

    @Autowired private LoadDao loadDao;
    @Autowired private ConfigurationService configService;
    @Autowired private GoogleResourceConfiguration resourceConfiguration;
    @Autowired private GoogleBucketService bucketService;
    @Autowired private GoogleProjectService projectService;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private GoogleResourceDao resourceDao;
    @Autowired private ResourceService resourceService;
    @Autowired private ProfileService profileService;
    @Autowired private ConnectedTestConfiguration testConfig;
    @MockBean private IamProviderInterface samService;

    private BucketResourceUtils bucketResourceUtils = new BucketResourceUtils();
    private BillingProfileModel profile;
    private Storage storage;
    private List<String> bucketNames;
    private boolean allowReuseExistingBuckets;
    private GoogleProjectResource projectResource;
    @Before
    public void setup() throws Exception {
        logger.info("property allowReuseExistingBuckets = {}",
            bucketResourceUtils.getAllowReuseExistingBuckets(configService));

        profile = connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
        connectedOperations.stubOutSamCalls(samService);
        storage = StorageOptions.getDefaultInstance().getService();
        bucketNames = new ArrayList<>();

        // get or created project in which to do the bucket work
        projectResource = buildProjectResource();
    }

    @After
    public void teardown() throws Exception {
        for (String bucketName : bucketNames) {
            deleteBucket(bucketName);
        }
        // Connected operations resets the configuration
        connectedOperations.teardown();
    }

    @Test
    // create and delete the bucket, checking that the metadata and cloud state match what is expected
    public void createAndDeleteBucketTest() throws Exception {
        String bucketName = "testbucket_createanddeletebuckettest";
        String flightId = "createAndDeleteBucketTest";
        bucketNames.add(bucketName);

        // create the bucket and metadata
        GoogleBucketResource bucketResource = bucketService.getOrCreateBucket(bucketName, projectResource, flightId);

        // check the bucket and metadata exist
        checkBucketExists(bucketResource.getResourceId());

        // delete the bucket and metadata
        deleteBucket(bucketResource.getName());
        checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
    }

    @Test
    // two threads compete for the bucket lock, confirm one wins and one loses. a third thread fetches the bucket
    // after it's been created, confirm it succeeds.
    public void twoThreadsCompeteForLockTest() throws Exception {
        String flightIdBase = "twoThreadsCompeteForLockTest";
        String bucketName = "twothreadscompeteforlocktest";
        bucketNames.add(bucketName);

        BucketResourceLockTester resourceLockA = new BucketResourceLockTester(
            bucketService, bucketName, projectResource, flightIdBase + "A");
        BucketResourceLockTester resourceLockB = new BucketResourceLockTester(
            bucketService, bucketName, projectResource, flightIdBase + "B");
        BucketResourceLockTester resourceLockC = new BucketResourceLockTester(
            bucketService, bucketName, projectResource, flightIdBase + "C");

        Thread threadA = new Thread(resourceLockA);
        Thread threadB = new Thread(resourceLockB);
        Thread threadC = new Thread(resourceLockC);

        configService.setFault(ConfigEnum.BUCKET_LOCK_CONFLICT_STOP_FAULT.name(), true);
        threadA.start();
        TimeUnit.SECONDS.sleep(1);

        threadB.start();
        threadB.join();
        assertTrue("Thread B did get a lock exception", resourceLockB.gotLockException());

        configService.setFault(ConfigEnum.BUCKET_LOCK_CONFLICT_CONTINUE_FAULT.name(), true);
        threadA.join();
        assertFalse("Thread A did not get a lock exception", resourceLockA.gotLockException());

        GoogleBucketResource bucketResource = resourceLockA.getBucketResource();
        assertNotNull("Thread A did create the bucket", bucketResource);

        checkBucketExists(bucketResource.getResourceId());

        threadC.start();
        threadC.join();
        assertFalse("Thread C did not get a lock exception", resourceLockC.gotLockException());
        assertNotNull("Thread C did get the bucket", resourceLockC.getBucketResource());

        deleteBucket(bucketResource.getName());
        checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
    }

    @Test
    // this is testing one case of corrupt metadata (i.e. state of the cloud does not match state of the metadata)
    // bucket cloud resource exists, but the corresponding bucket_resource metadata row does not
    public void bucketExistsBeforeMetadataTest() throws Exception {
        logger.info("property allowReuseExistingBuckets = {}",
            bucketResourceUtils.getAllowReuseExistingBuckets(configService));

        String bucketName = "testbucket_bucketexistsbeforemetadatatest";
        String flightIdA = "bucketExistsBeforeMetadataTestA";
        bucketNames.add(bucketName);

        // create the bucket and metadata
        GoogleBucketResource bucketResource = bucketService.getOrCreateBucket(bucketName, projectResource, flightIdA);
        checkBucketExists(bucketResource.getResourceId());

        // delete the metadata only
        boolean rowDeleted = resourceDao.deleteBucketMetadata(bucketName, flightIdA);
        assertTrue("metadata row deleted", rowDeleted);

        // try to fetch the bucket again, check fails with not found exception
        boolean caughtNotFoundException = false;
        try {
            bucketService.getBucketResourceById(bucketResource.getResourceId(), true);
        } catch (GoogleResourceNotFoundException cmEx) {
            caughtNotFoundException = true;
        }
        assertTrue("fetch failed when metadata does not exist", caughtNotFoundException);

        // set application property allowReuseExistingBuckets=false
        // try to create bucket again, check fails with corrupt metadata exception
        bucketResourceUtils.setAllowReuseExistingBuckets(configService, false);
        String flightIdB = "bucketExistsBeforeMetadataTestB";
        boolean caughtCorruptMetadataException = false;
        try {
            bucketService.getOrCreateBucket(bucketName, projectResource, flightIdB);
        } catch (CorruptMetadataException cmEx) {
            caughtCorruptMetadataException = true;
        }
        assertTrue("create failed when cloud resource already exists", caughtCorruptMetadataException);

        // set application property allowReuseExistingBuckets=true
        // try to create bucket again, check succeeds
        bucketResourceUtils.setAllowReuseExistingBuckets(configService, true);
        String flightIdC = "bucketExistsBeforeMetadataTestC";
        bucketResource = bucketService.getOrCreateBucket(bucketName, projectResource, flightIdC);

        // check the bucket and metadata exist
        checkBucketExists(bucketResource.getResourceId());

        // delete the bucket and metadata
        deleteBucket(bucketResource.getName());
        checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
    }

    @Test
    // this is testing one case of corrupt metadata (i.e. state of the cloud does not match state of the metadata)
    // bucket_resource metadata row exists, but the corresponding bucket cloud resource does not
    public void noBucketButMetadataExistsTest() throws Exception {
        logger.info("property allowReuseExistingBuckets = {}",
            bucketResourceUtils.getAllowReuseExistingBuckets(configService));

        String bucketName = "testbucket_nobucketbutmetadataexiststest";
        String flightIdA = "noBucketButMetadataExistsTestA";
        bucketNames.add(bucketName);

        // create the bucket and metadata
        GoogleBucketResource bucketResource = bucketService.getOrCreateBucket(bucketName, projectResource, flightIdA);
        checkBucketExists(bucketResource.getResourceId());

        // delete the bucket cloud resource only
        Bucket bucket = storage.get(bucketName);
        boolean bucketDeleted = bucket.delete();
        assertTrue("bucket cloud resource deleted", bucketDeleted);

        // try to fetch the bucket again, check fails with corrupt metadata exception
        boolean caughtCorruptMetadataException = false;
        try {
            bucketService.getBucketResourceById(bucketResource.getResourceId(), true);
        } catch (CorruptMetadataException cmEx) {
            caughtCorruptMetadataException = true;
        }
        assertTrue("fetch failed when cloud resource does not exist", caughtCorruptMetadataException);

        // try to getOrCreate bucket again, check fails with corrupt metadata exception
        String flightIdB = "bucketExistsBeforeMetadataTestB";
        caughtCorruptMetadataException = false;
        try {
            bucketService.getOrCreateBucket(bucketName, projectResource, flightIdB);
        } catch (CorruptMetadataException cmEx) {
            caughtCorruptMetadataException = true;
        }
        assertTrue("getOrCreate failed when cloud resource does not exist", caughtCorruptMetadataException);

        // update the metadata to match the cloud state, check that everything is deleted
        bucketService.updateBucketMetadata(bucketName, null);
        checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
    }

    private void checkBucketExists(UUID bucketResourceId) {
        // confirm the metadata row is unlocked and the bucket exists
        GoogleBucketResource bucketResource = bucketService.getBucketResourceById(bucketResourceId, false);
        assertNotNull("bucket metadata row exists", bucketResource);
        assertNull("bucket metadata is unlocked", bucketResource.getFlightId());

        Bucket bucket = storage.get(bucketResource.getName());
        assertNotNull("bucket exists in the cloud", bucket);
    }

    private void deleteBucket(String bucketName) {
        // delete the bucket and update the metadata
        storage.delete(bucketName);
        bucketService.updateBucketMetadata(bucketName, null);
    }

    private void checkBucketDeleted(String bucketName, UUID bucketResourceId) {
        // confirm the bucket and metadata row no longer exist
        Bucket bucket = storage.get(bucketName);
        assertNull("bucket no longer exists", bucket);

        boolean exceptionThrown = false;
        try {
            GoogleBucketResource bucketResource = bucketService.getBucketResourceById(bucketResourceId, false);
            logger.info("bucketResource = " + bucketResource);
        } catch (GoogleResourceNotFoundException grnfEx) {
            exceptionThrown = true;
        }
        assertTrue("bucket metadata row no longer exists", exceptionThrown);
    }

    private GoogleProjectResource buildProjectResource() throws Exception {
        String role = "roles/bigquery.jobUser";
        String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
        List<String> stewardsGroupEmailList = Lists.newArrayList();
        stewardsGroupEmailList.add(stewardsGroupEmail);
        Map<String, List<String>> roleToStewardMap = new HashMap<>();
        roleToStewardMap.put(role, stewardsGroupEmailList);

        // create project metadata
        return projectService.getOrCreateProject(
                resourceConfiguration.getSingleDataProjectId(),
                profile,
                roleToStewardMap);
    }

}
