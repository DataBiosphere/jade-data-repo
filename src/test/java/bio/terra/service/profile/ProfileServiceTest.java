package bio.terra.service.profile;


import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.load.LoadDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.BucketResourceUtils;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import com.google.api.client.util.Lists;
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

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;


@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class ProfileServiceTest {
    private final Logger logger = LoggerFactory.getLogger(ProfileServiceTest.class);

    @Autowired
    private LoadDao loadDao;
    @Autowired
    private ConfigurationService configService;
    @Autowired
    private GoogleResourceConfiguration resourceConfiguration;
    @Autowired
    private GoogleBucketService bucketService;
    @Autowired
    private GoogleProjectService projectService;
    @Autowired
    private ConnectedOperations connectedOperations;
    @Autowired
    private GoogleResourceDao resourceDao;
    @Autowired
    private ResourceService resourceService;
    @Autowired
    private ProfileService profileService;
    @Autowired
    private ConnectedTestConfiguration testConfig;
    @MockBean
    private IamProviderInterface samService;

    private BucketResourceUtils bucketResourceUtils = new BucketResourceUtils();
    private BillingProfileModel profile;
    private Storage storage;
    private List<String> bucketNames;
    private boolean allowReuseExistingBuckets;
    private GoogleProjectResource projectResource;
    private String oldBillingAccountId;
    private String newBillingAccountId;


    @Before
    public void setup() throws Exception {
        System.out.println("================== SETUP ===================");
        oldBillingAccountId = testConfig.getGoogleBillingAccountId();
        newBillingAccountId = testConfig.getSecondGoogleBillingAccountId();

        profile = connectedOperations.createProfileForAccount(oldBillingAccountId);
        connectedOperations.stubOutSamCalls(samService);
        storage = StorageOptions.getDefaultInstance().getService();
        bucketNames = new ArrayList<>();

        // get or created project in which to do the bucket work
        projectResource = buildProjectResource();
        System.out.println("================== TEST ===================");
    }

    @After
    public void teardown() throws Exception {
        logger.info("================== CLEANUP ===================");
        // Connected operations resets the configuration
        connectedOperations.teardown();
    }

    @Test
    // create and delete the bucket, checking that the metadata and cloud state match what is expected
    public void createAndUpdateProfileTest() throws Exception {
        System.out.println("profile: " + profile.getProfileName());
        BillingProfileModel model = connectedOperations.getProfileById(profile.getId());
        System.out.println("Retrieve Profile: " + model.getProfileName());

        // ==========THING TESTED FOR THIS PR=============
        BillingProfileRequestModel updatedRequest = new BillingProfileRequestModel()
            .billingAccountId(newBillingAccountId)
            .biller("direct")
            .description("updated profile")
            .id(profile.getId())
            .profileName(profile.getProfileName() + "-updated");
        // TODO: Can the profile name change? Since we create a project that is tied between the two, I don't think so.
        // probably should create new "BillingProfileUpdateRequestModel" that just allow changes to billing account id
        // and description?

        // This just submits the update profile job
        BillingProfileModel newModel = connectedOperations.updateProfile(updatedRequest);
        logger.info("Updated model: {}", newModel.toString());
        assertThat("Billing account should be equal to the newBillingAccountId",
            newModel.getBillingAccountId(),
            equalTo(newBillingAccountId));

        // ==========THING TESTED FOR THIS PR=============
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
