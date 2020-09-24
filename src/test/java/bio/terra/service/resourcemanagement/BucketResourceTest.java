package bio.terra.service.resourcemanagement;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.load.LoadDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.google.GoogleBucketRequest;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectRequest;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
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
    @Autowired private GoogleResourceService resourceService;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private GoogleResourceDao resourceDao;
    @Autowired private DataLocationService dataLocationService;
    @Autowired private ProfileService profileService;
    @Autowired private ConnectedTestConfiguration testConfig;
    @MockBean private IamProviderInterface samService;

    private BillingProfileModel profile;
    private Storage storage;
    private List<String> bucketNames;
    private boolean allowReuseExistingBuckets;

    @Before
    public void setup() throws Exception {
        // save original value of application property allowReuseExistingBuckets
        allowReuseExistingBuckets = resourceService.getAllowReuseExistingBuckets();
        logger.info("app property allowReuseExistingBuckets = " + resourceService.getAllowReuseExistingBuckets());

        profile = connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
        connectedOperations.stubOutSamCalls(samService);
        storage = StorageOptions.getDefaultInstance().getService();
        bucketNames = new ArrayList<>();
    }

    @After
    public void teardown() throws Exception {
        // restore original value of application property allowReuseExistingBuckets
        resourceService.setAllowReuseExistingBuckets(allowReuseExistingBuckets);
        logger.info("app property allowReuseExistingBuckets = " + resourceService.getAllowReuseExistingBuckets());

        for (String bucketName : bucketNames) {
            deleteBucket(bucketName);
        }
        connectedOperations.teardown();
    }

    @Test
    // create and delete the bucket, checking that the metadata and cloud state match what is expected
    public void createAndDeleteBucketTest() throws Exception {
        String bucketName = "testbucket_createanddeletebuckettest";
        String flightId = "createAndDeleteBucketTest";
        bucketNames.add(bucketName);

        // create the bucket and metadata
        GoogleBucketRequest googleBucketRequest = buildBucketRequest(bucketName);
        GoogleBucketResource bucketResource = resourceService.getOrCreateBucket(googleBucketRequest, flightId);

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

        GoogleBucketRequest bucketRequest = buildBucketRequest(bucketName);
        BucketResourceLockTester resourceLockA = new BucketResourceLockTester(
            resourceService, bucketRequest, flightIdBase + "A");
        BucketResourceLockTester resourceLockB = new BucketResourceLockTester(
            resourceService, bucketRequest, flightIdBase + "B");
        BucketResourceLockTester resourceLockC = new BucketResourceLockTester(
            resourceService, bucketRequest, flightIdBase + "C");

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
        logger.info("app property allowReuseExistingBuckets = " + resourceService.getAllowReuseExistingBuckets());

        String bucketName = "testbucket_bucketexistsbeforemetadatatest";
        String flightIdA = "bucketExistsBeforeMetadataTestA";
        bucketNames.add(bucketName);

        // create the bucket and metadata
        GoogleBucketRequest googleBucketRequest = buildBucketRequest(bucketName);
        GoogleBucketResource bucketResource = resourceService.getOrCreateBucket(googleBucketRequest, flightIdA);
        checkBucketExists(bucketResource.getResourceId());

        // delete the metadata only
        boolean rowDeleted = resourceDao.deleteBucketMetadata(bucketName, flightIdA);
        assertTrue("metadata row deleted", rowDeleted);

        // try to fetch the bucket again, check fails with not found exception
        boolean caughtNotFoundException = false;
        try {
            resourceService.getBucketResourceById(bucketResource.getResourceId(), true);
        } catch (GoogleResourceNotFoundException cmEx) {
            caughtNotFoundException = true;
        }
        assertTrue("fetch failed when metadata does not exist", caughtNotFoundException);

        // set application property allowReuseExistingBuckets=false
        // try to create bucket again, check fails with corrupt metadata exception
        resourceService.setAllowReuseExistingBuckets(false);
        String flightIdB = "bucketExistsBeforeMetadataTestB";
        boolean caughtCorruptMetadataException = false;
        try {
            resourceService.getOrCreateBucket(googleBucketRequest, flightIdB);
        } catch (CorruptMetadataException cmEx) {
            caughtCorruptMetadataException = true;
        }
        assertTrue("create failed when cloud resource already exists", caughtCorruptMetadataException);

        // set application property allowReuseExistingBuckets=true
        // try to create bucket again, check succeeds
        resourceService.setAllowReuseExistingBuckets(true);
        String flightIdC = "bucketExistsBeforeMetadataTestC";
        bucketResource = resourceService.getOrCreateBucket(googleBucketRequest, flightIdC);

        // check the bucket and metadata exist
        checkBucketExists(bucketResource.getResourceId());

        // delete the bucket and metadata
        deleteBucket(bucketResource.getName());
        checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());

        // restore original value of application property allowReuseExistingBuckets, which was saved in setup
        // (this is also done in cleanup after all tests, in case this test errors out before reaching this line)
        resourceService.setAllowReuseExistingBuckets(allowReuseExistingBuckets);
    }

    @Test
    // this is testing one case of corrupt metadata (i.e. state of the cloud does not match state of the metadata)
    // bucket_resource metadata row exists, but the corresponding bucket cloud resource does not
    public void noBucketButMetadataExistsTest() throws Exception {
        logger.info("app property allowReuseExistingBuckets = " + resourceService.getAllowReuseExistingBuckets());

        String bucketName = "testbucket_nobucketbutmetadataexiststest";
        String flightIdA = "noBucketButMetadataExistsTestA";
        bucketNames.add(bucketName);

        // create the bucket and metadata
        GoogleBucketRequest googleBucketRequest = buildBucketRequest(bucketName);
        GoogleBucketResource bucketResource = resourceService.getOrCreateBucket(googleBucketRequest, flightIdA);
        checkBucketExists(bucketResource.getResourceId());

        // delete the bucket cloud resource only
        Bucket bucket = storage.get(bucketName);
        boolean bucketDeleted = bucket.delete();
        assertTrue("bucket cloud resource deleted", bucketDeleted);

        // try to fetch the bucket again, check fails with corrupt metadata exception
        boolean caughtCorruptMetadataException = false;
        try {
            resourceService.getBucketResourceById(bucketResource.getResourceId(), true);
        } catch (CorruptMetadataException cmEx) {
            caughtCorruptMetadataException = true;
        }
        assertTrue("fetch failed when cloud resource does not exist", caughtCorruptMetadataException);

        // try to getOrCreate bucket again, check fails with corrupt metadata exception
        String flightIdB = "bucketExistsBeforeMetadataTestB";
        caughtCorruptMetadataException = false;
        try {
            resourceService.getOrCreateBucket(googleBucketRequest, flightIdB);
        } catch (CorruptMetadataException cmEx) {
            caughtCorruptMetadataException = true;
        }
        assertTrue("getOrCreate failed when cloud resource does not exist", caughtCorruptMetadataException);

        // update the metadata to match the cloud state, check that everything is deleted
        resourceService.updateBucketMetadata(bucketName, null);
        checkBucketDeleted(bucketResource.getName(), bucketResource.getResourceId());
    }

    private void checkBucketExists(UUID bucketResourceId) {
        // confirm the metadata row is unlocked and the bucket exists
        GoogleBucketResource bucketResource = resourceService.getBucketResourceById(bucketResourceId, false);
        assertNotNull("bucket metadata row exists", bucketResource);
        assertNull("bucket metadata is unlocked", bucketResource.getFlightId());

        Bucket bucket = storage.get(bucketResource.getName());
        assertNotNull("bucket exists in the cloud", bucket);
    }

    private void deleteBucket(String bucketName) {
        // delete the bucket and update the metadata
        storage.delete(bucketName);
        resourceService.updateBucketMetadata(bucketName, null);
    }

    private void checkBucketDeleted(String bucketName, UUID bucketResourceId) {
        // confirm the bucket and metadata row no longer exist
        Bucket bucket = storage.get(bucketName);
        assertNull("bucket no longer exists", bucket);

        boolean exceptionThrown = false;
        try {
            GoogleBucketResource bucketResource = resourceService.getBucketResourceById(bucketResourceId, false);
            logger.info("bucketResource = " + bucketResource);
        } catch (GoogleResourceNotFoundException grnfEx) {
            exceptionThrown = true;
        }
        assertTrue("bucket metadata row no longer exists", exceptionThrown);
    }

    private GoogleBucketRequest buildBucketRequest(String bucketName) throws Exception {
        // build project request
        String role = "roles/bigquery.jobUser";
        String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
        List<String> stewardsGroupEmailList = Lists.newArrayList();
        stewardsGroupEmailList.add(stewardsGroupEmail);
        Map<String, List<String>> roleToStewardMap = new HashMap();
        roleToStewardMap.put(role, stewardsGroupEmailList);

        GoogleProjectRequest projectRequest = new GoogleProjectRequest()
            .projectId(resourceConfiguration.getProjectId() + "-data")
            .profileId(UUID.fromString(profile.getId()))
            .serviceIds(DataLocationService.DATA_PROJECT_SERVICE_IDS)
            .roleIdentityMapping(roleToStewardMap);

        // create project metadata
        GoogleProjectResource projectResource = resourceService.getOrCreateProject(projectRequest);

        // create the bucket request
        BillingProfile billingProfile = profileService.getProfileById(UUID.fromString(profile.getId()));
        GoogleBucketRequest googleBucketRequest = new GoogleBucketRequest()
            .googleProjectResource(projectResource)
            .bucketName(bucketName)
            .profileId(billingProfile.getId())
            .region(billingProfile.getGcsRegion());

        return googleBucketRequest;
    }

}
