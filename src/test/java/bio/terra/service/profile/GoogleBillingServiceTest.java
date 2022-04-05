package bio.terra.service.profile;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import com.google.api.client.util.Lists;
import com.google.cloud.billing.v1.ProjectBillingInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class GoogleBillingServiceTest {
  private final Logger logger = LoggerFactory.getLogger(GoogleBillingServiceTest.class);

  @Autowired private GoogleProjectService projectService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private GoogleBillingService googleBillingService;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private BufferService bufferService;

  @MockBean private IamProviderInterface samService;

  private BillingProfileModel profile;
  private GoogleProjectResource projectResource;
  private String oldBillingAccountId;
  private String newBillingAccountId;

  @Before
  public void setup() throws Exception {
    // The "no spend" google account is the default account in the RBS tools environment
    oldBillingAccountId = testConfig.getNoSpendGoogleBillingAccountId();
    // Alternate billing account that we should use for testing
    newBillingAccountId = testConfig.getGoogleBillingAccountId();

    assertThat(
        "Testing against two different billing accounts",
        oldBillingAccountId,
        not(newBillingAccountId));

    // We want to create the project with this alternate billing account
    // Confirm that on project create, the billing account is switched to this account
    profile = connectedOperations.createProfileForAccount(newBillingAccountId);
    connectedOperations.stubOutSamCalls(samService);

    // get or created project in which to do the bucket work
    projectResource = buildProjectResource();
  }

  @After
  public void teardown() throws Exception {
    // Connected operations resets the configuration
    connectedOperations.teardown();
  }

  @Test
  public void getBillingAccount() {
    assertThat(
        "Billing account is set to the account provided in setup.",
        profile.getBillingAccountId(),
        equalTo(newBillingAccountId));
    ProjectBillingInfo billingAccount =
        googleBillingService.getProjectBilling(projectResource.getGoogleProjectId());
    logger.info(
        "Billing Account ID {} for Project {}",
        billingAccount.getBillingAccountName(),
        projectResource.getGoogleProjectId());
    assertThat(
        "Billing account should be equal to the one we set.",
        billingAccount.getBillingAccountName(),
        containsString(profile.getBillingAccountId()));
  }

  @Test
  public void assignProjectBilling() {
    // Check state before Assigning new billing account Id
    ProjectBillingInfo billingAccount =
        googleBillingService.getProjectBilling(projectResource.getGoogleProjectId());
    assertThat(
        "Billing account should be equal to the 'new' billing account.",
        billingAccount.getBillingAccountName(),
        containsString(newBillingAccountId));
    logger.info(
        "Before assigning project billing: Billing Account Name {} for Project {}",
        billingAccount.getBillingAccountName(),
        projectResource.getGoogleProjectId());

    // Assign a different Billing Account to the existing Project --
    // Attempt to assign the project back to the default RBS tools account id,
    // confirming we are able to update the billing account
    BillingProfileModel newBillingProfile = new BillingProfileModel();
    newBillingProfile.setBillingAccountId(oldBillingAccountId);
    boolean billingEnabled =
        googleBillingService.assignProjectBilling(newBillingProfile, projectResource);
    assertTrue("Billing should be enabled after updating the billing account", billingEnabled);

    // Check if the change was successful
    ProjectBillingInfo updatedBillingAccount =
        googleBillingService.getProjectBilling(projectResource.getGoogleProjectId());
    logger.info(
        "After assigning project billing: Billing Account Name {} for Project {}",
        updatedBillingAccount.getBillingAccountName(),
        projectResource.getGoogleProjectId());
    assertThat(
        "Billing account should be equal to the one we set.",
        updatedBillingAccount.getBillingAccountName(),
        containsString(oldBillingAccountId));
  }

  private GoogleProjectResource buildProjectResource() throws Exception {
    String role = "roles/bigquery.jobUser";
    String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
    List<String> stewardsGroupEmailList = Lists.newArrayList();
    stewardsGroupEmailList.add(stewardsGroupEmail);
    Map<String, List<String>> roleToStewardMap = new HashMap<>();
    roleToStewardMap.put(role, stewardsGroupEmailList);

    ResourceInfo resourceInfo = bufferService.handoutResource(false);
    // create project metadata
    return projectService.initializeGoogleProject(
        resourceInfo.getCloudResourceUid().getGoogleProjectUid().getProjectId(),
        profile,
        roleToStewardMap,
        GoogleRegion.DEFAULT_GOOGLE_REGION,
        Map.of("test-name", "google-billing-service-test"));
  }
}
