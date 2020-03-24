package bio.terra.service.resourcemanagement;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamService;
import bio.terra.service.load.LoadDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.google.GoogleBucketRequest;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectRequest;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
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
public class ResourceLockTest {
    private final Logger logger = LoggerFactory.getLogger(ResourceLockTest.class);

    @Autowired private LoadDao loadDao;
    @Autowired private ConfigurationService configService;
    @Autowired private GoogleResourceConfiguration resourceConfiguration;
    @Autowired private GoogleResourceService resourceService;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private GoogleResourceDao resourceDao;
    @Autowired private DataLocationService dataLocationService;
    @Autowired private ProfileService profileService;
    @MockBean private IamService samService;

    private BillingProfileModel profile;
    private Storage storage;

    @Before
    public void setup() throws Exception {
        profile = connectedOperations.createProfileForAccount(resourceConfiguration.getCoreBillingAccount());
        connectedOperations.stubOutSamCalls(samService);
        storage = StorageOptions.getDefaultInstance().getService();
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    @Test
    public void createAndDeleteBucketTest() {
        String bucketName = "testbucket_createanddeletebuckettest";
        String flightId = "testFlightId";

        // create the bucket and metadata
        GoogleBucketRequest googleBucketRequest = buildBucketRequest(bucketName);
        GoogleBucketResource bucketResource = resourceService.getOrCreateBucket(googleBucketRequest, flightId);

        // confirm the bucket exists and the metadata row is unlocked
        Bucket bucket = storage.get(bucketName);
        assertNotNull("bucket exists in the cloud", bucket);

        bucketResource = resourceService.getBucketResourceById(bucketResource.getResourceId());
        assertNotNull("bucket metadata row exists", bucketResource);
        assertNull("bucket metadata is unlocked", bucketResource.getFlightId());

        // delete the bucket and update the metadata
        storage.delete(bucketName);
        resourceService.updateBucketMetadata(bucketName, null);

        // confirm the bucket and metadata row no longer exist
        bucket = storage.get(bucketName);
        assertNull("bucket no longer exists", bucket);

        boolean exceptionThrown = false;
        try {
            bucketResource = resourceService.getBucketResourceById(bucketResource.getResourceId());
            logger.info("bucketResource = " + bucketResource);
        } catch (GoogleResourceNotFoundException grnfEx) {
            exceptionThrown = true;
        }
        assertTrue("bucket metadata row no longer exists", exceptionThrown);
    }

    @Test
    public void twoThreadsCompeteForLockTest() throws Exception {
        String flightIdBase = "twoThreadsCompeteForLockTest";
        String bucketName = "twothreadscompeteforlocktest";
        GoogleBucketRequest bucketRequest = buildBucketRequest(bucketName);
        ResourceLockTester resourceLockA = new ResourceLockTester(resourceService, bucketRequest, flightIdBase + "A");
        ResourceLockTester resourceLockB = new ResourceLockTester(resourceService, bucketRequest, flightIdBase + "B");
        ResourceLockTester resourceLockC = new ResourceLockTester(resourceService, bucketRequest, flightIdBase + "C");

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

        deleteBucket(bucketResource.getName(), bucketResource.getResourceId());
    }

    private void checkBucketExists(UUID bucketResourceId) {
        // confirm the metadata row is unlocked and the bucket exists
        GoogleBucketResource bucketResource = resourceService.getBucketResourceById(bucketResourceId);
        assertNotNull("bucket metadata row exists", bucketResource);
        assertNull("bucket metadata is unlocked", bucketResource.getFlightId());

        Bucket bucket = storage.get(bucketResource.getName());
        assertNotNull("bucket exists in the cloud", bucket);
    }

    private void deleteBucket(String bucketName, UUID bucketResourceId) {
        // delete the bucket and update the metadata
        storage.delete(bucketName);
        resourceService.updateBucketMetadata(bucketName, null);

        // confirm the bucket and metadata row no longer exist
        Bucket bucket = storage.get(bucketName);
        assertNull("bucket no longer exists", bucket);

        boolean exceptionThrown = false;
        try {
            GoogleBucketResource bucketResource = resourceService.getBucketResourceById(bucketResourceId);
            logger.info("bucketResource = " + bucketResource);
        } catch (GoogleResourceNotFoundException grnfEx) {
            exceptionThrown = true;
        }
        assertTrue("bucket metadata row no longer exists", exceptionThrown);
    }

    private GoogleBucketRequest buildBucketRequest(String bucketName) {
        // build project request
        String role = "roles/bigquery.jobUser";
        String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
        List<String> stewardsGroupEmailList = Lists.newArrayList();
        stewardsGroupEmailList.add(stewardsGroupEmail);
        Map<String, List<String>> roleToStewardMap = new HashMap();
        roleToStewardMap.put(role, stewardsGroupEmailList);

        GoogleProjectRequest projectRequest = new GoogleProjectRequest()
            .projectId(resourceConfiguration.getProjectId())
            .profileId(UUID.fromString(profile.getId()))
            .serviceIds(DataLocationService.DATA_PROJECT_SERVICE_IDS)
            .roleIdentityMapping(roleToStewardMap);

        // create project metadata
        GoogleProjectResource projectResource = resourceService.getOrCreateProject(projectRequest);

        // create the bucket and metadata
        BillingProfile billingProfile = profileService.getProfileById(UUID.fromString(profile.getId()));
        GoogleBucketRequest googleBucketRequest = new GoogleBucketRequest()
            .googleProjectResource(projectResource)
            .bucketName(bucketName)
            .profileId(billingProfile.getId())
            .region(billingProfile.getGcsRegion());

        return googleBucketRequest;
    }

}
