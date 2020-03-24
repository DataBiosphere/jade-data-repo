package bio.terra.service.resourcemanagement;


import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.google.GoogleBucketRequest;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectRequest;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
import com.google.api.client.util.Lists;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class ResourceServiceTest {

    @Autowired private GoogleResourceConfiguration resourceConfiguration;
    @Autowired private GoogleResourceService resourceService;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private GoogleResourceDao resourceDao;
    @Autowired private DataLocationService dataLocationService;
    @Autowired private ProfileService profileService;

    private BillingProfileModel profile;

    @Before
    public void setup() throws Exception {
        profile = connectedOperations.createProfileForAccount(resourceConfiguration.getCoreBillingAccount());
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    @Test
    // this test should be unignored whenever there are changes to the project creation or deletion code
    @Ignore
    public void createAndDeleteProjectTest() {
        // the project id can't be more than 30 characters
        String projectId = ("test-" + UUID.randomUUID().toString()).substring(0, 30);

        String role = "roles/bigquery.jobUser";
        String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
        List<String> stewardsGroupEmailList = Lists.newArrayList();
        stewardsGroupEmailList.add(stewardsGroupEmail);
        Map<String, List<String>> roleToStewardMap = new HashMap();
        roleToStewardMap.put(role, stewardsGroupEmailList);

        GoogleProjectRequest projectRequest = new GoogleProjectRequest()
            .projectId(projectId)
            .profileId(UUID.fromString(profile.getId()))
            .serviceIds(DataLocationService.DATA_PROJECT_SERVICE_IDS)
            .roleIdentityMapping(roleToStewardMap);
        GoogleProjectResource projectResource = resourceService.getOrCreateProject(projectRequest);

        Project project = resourceService.getProject(projectId);
        assertThat("the project is active",
            project.getLifecycleState(),
            equalTo("ACTIVE"));

        // TODO check to make sure a steward can complete a job in another test

        resourceService.deleteProjectResource(projectResource.getRepositoryId());
        project = resourceService.getProject(projectId);
        assertThat("the project is not active after delete",
            project.getLifecycleState(),
            not(equalTo("ACTIVE")));
    }

    @Test
    public void createAndDeleteBucketTest() {
        String bucketName = "testbucket_createanddeletebuckettest";
        String flightId = "testFlightId";
        Storage storage = StorageOptions.getDefaultInstance().getService();

        // create project metadata
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
        GoogleProjectResource projectResource = resourceService.getOrCreateProject(projectRequest);

        // create the bucket and metadata
        BillingProfile billingProfile = profileService.getProfileById(UUID.fromString(profile.getId()));
        GoogleBucketRequest googleBucketRequest = new GoogleBucketRequest()
            .googleProjectResource(projectResource)
            .bucketName(bucketName)
            .profileId(billingProfile.getId())
            .region(billingProfile.getGcsRegion());
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
            System.out.println("bucketResource = " + bucketResource);
        } catch (GoogleResourceNotFoundException grnfEx) {
            exceptionThrown = true;
        }
        assertTrue("bucket metadata row no longer exists", exceptionThrown);
    }
}
