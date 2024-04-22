package bio.terra.service.profile;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.CollectionType;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.ErrorModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class ProfileServiceTest {
  private final Logger logger = LoggerFactory.getLogger(ProfileServiceTest.class);

  @Autowired private GoogleBillingService googleBillingService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private GoogleResourceDao googleResourceDao;
  @Autowired private ProfileDao profileDao;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private GoogleProjectService googleProjectService;
  @Autowired private ProfileService profileService;
  @Autowired private BufferService bufferService;
  @MockBean private IamProviderInterface samService;

  private BillingProfileModel profile;
  private GoogleProjectResource projectResource;
  private String oldBillingAccountId;
  private String newBillingAccountId;
  private List<BillingProfileModel> profiles = new ArrayList<>();

  @Before
  public void setup() throws Exception {
    oldBillingAccountId = testConfig.getGoogleBillingAccountId();
    newBillingAccountId = testConfig.getNoSpendGoogleBillingAccountId();

    profile = connectedOperations.createProfileForAccount(oldBillingAccountId);
    profiles.add(profile);
    connectedOperations.stubOutSamCalls(samService);

    projectResource = buildProjectResource();
  }

  @After
  public void teardown() throws Exception {
    googleBillingService.assignProjectBilling(profile, projectResource);
    googleResourceDao.deleteProject(projectResource.getId());
    profiles.forEach(profile -> profileDao.deleteBillingProfileById(profile.getId()));
    // Connected operations resets the configuration
    connectedOperations.teardown();
  }

  @Ignore(
      "Test to update the billing account on google project, so ignoring until we can test by creating a "
          + "new project, test changing the billing account, and then delete the project")
  @Test
  public void updateProfileTest() throws Exception {
    logger.debug("profile: " + profile.getProfileName());
    BillingProfileModel model = profileService.getProfileByIdNoCheck(profile.getId());
    assertThat(
        "BEFORE UPDATE: Billing account should be equal to the oldBillingAccountId",
        model.getBillingAccountId(),
        equalTo(oldBillingAccountId));
    logger.debug("Retrieve Profile: " + model.getProfileName());

    BillingProfileUpdateModel updatedRequest =
        new BillingProfileUpdateModel()
            .billingAccountId(newBillingAccountId)
            .description("updated profile description")
            .id(profile.getId());

    BillingProfileModel newModel = connectedOperations.updateProfile(updatedRequest);
    logger.debug("Updated model: {}", newModel.toString());
    assertThat(
        "AFTER UPDATE: Billing account should be equal to the newBillingAccountId",
        newModel.getBillingAccountId(),
        equalTo(newBillingAccountId));
  }

  @Test
  public void testValidationUpdateProfileTest() throws Exception {
    BillingProfileUpdateModel badRequest =
        new BillingProfileUpdateModel()
            // .billingAccountId(newBillingAccountId)
            .description("updated profile description")
            .id(profile.getId());

    ErrorModel model =
        connectedOperations.updateProfileExpectError(badRequest, HttpStatus.BAD_REQUEST);
    logger.info(model.toString());
    assertThat(
        "Error Model message should contain validation error",
        model.getMessage(),
        containsString("Validation errors"));
    assertThat(
        "There should be 2 errors: 1 for null errors, 1 for incorrect pattern for billing account",
        model.getErrorDetail().size(),
        equalTo(2));
  }

  @Test
  public void testCreateProfileAddsProfileIdByDefault() throws Exception {
    BillingProfileRequestModel requestWithoutId =
        new BillingProfileRequestModel()
            .biller("direct")
            .billingAccountId(oldBillingAccountId)
            .profileName(UUID.randomUUID().toString())
            .description("profile description");
    BillingProfileModel profile = connectedOperations.createProfile(requestWithoutId);
    profiles.add(profile);
    assertNotNull(profile.getId());
  }

  @Test
  public void testValidationCreateProfileTest() throws Exception {
    BillingProfileRequestModel badRequest =
        new BillingProfileRequestModel()
            // .biller("direct")
            // .billingAccountId(newBillingAccountId)
            // .profileName("name")
            .description("updated profile description")
            .id(profile.getId());

    ErrorModel model =
        connectedOperations.createProfileExpectError(badRequest, HttpStatus.BAD_REQUEST);
    logger.info(model.toString());
    assertThat(
        "Error Model message should contain validation error",
        model.getMessage(),
        containsString("Validation error"));
    assertThat(
        "There should be 3 errors: 2 for null errors, 1 for incorrect pattern for billing account",
        model.getErrorDetail().size(),
        equalTo(3));
  }

  private GoogleProjectResource buildProjectResource() throws Exception {
    ResourceInfo resourceInfo = bufferService.handoutResource(false);

    // create project metadata
    return googleProjectService.initializeGoogleProject(
        resourceInfo.getCloudResourceUid().getGoogleProjectUid().getProjectId(),
        profile,
        GoogleRegion.DEFAULT_GOOGLE_REGION,
        Map.of("test-name", "profile-service-test"),
        CollectionType.DATASET);
  }
}
