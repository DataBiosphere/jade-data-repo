package bio.terra.service.profile;


import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import com.google.api.client.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
    private GoogleBillingService googleBillingService;
    @Autowired
    private ConnectedOperations connectedOperations;
    @Autowired
    private GoogleResourceDao googleResourceDao;
    @Autowired
    private ProfileDao profileDao;
    @Autowired
    private ConnectedTestConfiguration testConfig;
    @Autowired
    private GoogleProjectService googleProjectService;
    @Autowired
    private GoogleResourceConfiguration resourceConfiguration;
    @MockBean
    private IamProviderInterface samService;


    private BillingProfileModel profile;
    private GoogleProjectResource projectResource;
    private String oldBillingAccountId;
    private String newBillingAccountId;


    @Before
    public void setup() throws Exception {
        oldBillingAccountId = testConfig.getGoogleBillingAccountId();
        newBillingAccountId = testConfig.getSecondGoogleBillingAccountId();

        profile = connectedOperations.createProfileForAccount(oldBillingAccountId);
        connectedOperations.stubOutSamCalls(samService);

        projectResource = buildProjectResource();
    }

    @After
    public void teardown() throws Exception {
        googleBillingService.assignProjectBilling(profile, projectResource);
        googleResourceDao.deleteProject(projectResource.getId());
        profileDao.deleteBillingProfileById(UUID.fromString(profile.getId()));
        // Connected operations resets the configuration
        connectedOperations.teardown();
    }

    // TODO: Add back once we can create a new google project to test with
    // Test for DR-1404 - Updates the billing account on google project
    @Ignore
    @Test
    public void updateProfileTest() throws Exception {
        System.out.println("profile: " + profile.getProfileName());
        BillingProfileModel model = connectedOperations.getProfileById(profile.getId());
        assertThat("BEFORE UPDATE: Billing account should be equal to the oldBillingAccountId",
            model.getBillingAccountId(),
            equalTo(oldBillingAccountId));
        System.out.println("Retrieve Profile: " + model.getProfileName());

        BillingProfileUpdateModel updatedRequest = new BillingProfileUpdateModel()
            .billingAccountId(newBillingAccountId)
            .description("updated profile description")
            .id(profile.getId());

        BillingProfileModel newModel = connectedOperations.updateProfile(updatedRequest);
        logger.info("Updated model: {}", newModel.toString());
        assertThat("AFTER UPDATEBilling account should be equal to the newBillingAccountId",
            newModel.getBillingAccountId(),
            equalTo(newBillingAccountId));
    }

    private GoogleProjectResource buildProjectResource() throws Exception {
        String role = "roles/bigquery.jobUser";
        String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
        List<String> stewardsGroupEmailList = Lists.newArrayList();
        stewardsGroupEmailList.add(stewardsGroupEmail);
        Map<String, List<String>> roleToStewardMap = new HashMap<>();
        roleToStewardMap.put(role, stewardsGroupEmailList);

        // create project metadata
        return googleProjectService.getOrCreateProject(
            resourceConfiguration.getSingleDataProjectId(),
            profile,
            roleToStewardMap);
    }
}
