package bio.terra.service.profile;

import static org.hamcrest.CoreMatchers.containsString;
 import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import com.google.api.client.util.Lists;
import com.google.cloud.billing.v1.ProjectBillingInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class GoogleBillingServiceTest {
  private final Logger logger = LoggerFactory.getLogger(GoogleBillingServiceTest.class);

  @Autowired private GoogleResourceConfiguration resourceConfiguration;
  @Autowired private GoogleProjectService projectService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private GoogleBillingService googleBillingService;
  @Autowired private ConnectedTestConfiguration testConfig;
  @MockBean private IamProviderInterface samService;

  private BillingProfileModel profile;
  private GoogleProjectResource projectResource;
  private String oldBillingAccountId;
  private String newBillingAccountId;
  private boolean resetBillingAccount;

  @Before
  public void setup() throws Exception {
    oldBillingAccountId = testConfig.getGoogleBillingAccountId();
    newBillingAccountId = testConfig.getNoSpendGoogleBillingAccountId();
    resetBillingAccount = false;

    profile = connectedOperations.createProfileForAccount(oldBillingAccountId);
    connectedOperations.stubOutSamCalls(samService);

    // get or created project in which to do the bucket work
    projectResource = buildProjectResource();
  }

  @After
  public void teardown() throws Exception {
    if (resetBillingAccount) {
      googleBillingService.assignProjectBilling(profile, projectResource);
    }
    // Connected operations resets the configuration
    connectedOperations.teardown();
  }

  @Test
  public void getBillingAccount() {
    System.out.println("billingAccountId: " + profile.getBillingAccountId());
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

  @Ignore(
      "assignProjectBilling manipulates billing accounts, so ignoring until we can test by creating a "
          + "new project, test changing the billing account, and then delete the project")
  @Test
  public void assignProjectBilling() {
    // Check state before Assigning new billing account Id
    ProjectBillingInfo billingAccount =
        googleBillingService.getProjectBilling(projectResource.getGoogleProjectId());
    assertThat(
        "Billing account should be equal to the 'old' billing account.",
        billingAccount.getBillingAccountName(),
        containsString(oldBillingAccountId));
    logger.info(
        "Before assigning project billing: Billing Account Name {} for Project {}",
        billingAccount.getBillingAccountName(),
        projectResource.getGoogleProjectId());

    // Assign a the new Billing Account to the existing Project
    BillingProfileModel newBillingProfile = new BillingProfileModel();
    newBillingProfile.setBillingAccountId(newBillingAccountId);
    boolean billingEnabled =
        googleBillingService.assignProjectBilling(newBillingProfile, projectResource);
    resetBillingAccount = true;
    assertTrue("Billing should be enabled after updating the billing account", billingEnabled);

    // Check if the change was successful
    ProjectBillingInfo newBillingAccount =
        googleBillingService.getProjectBilling(projectResource.getGoogleProjectId());
    logger.info(
        "After assigning project billing: Billing Account Name {} for Project {}",
        newBillingAccount.getBillingAccountName(),
        projectResource.getGoogleProjectId());
    assertThat(
        "Billing account should be equal to the one we set.",
        newBillingAccount.getBillingAccountName(),
        containsString(newBillingAccountId));
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
        roleToStewardMap,
        GoogleRegion.DEFAULT_GOOGLE_REGION);
  }
}
