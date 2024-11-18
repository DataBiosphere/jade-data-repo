package bio.terra.service.resourcemanagement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.startsWith;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.ValidationUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.CloudPlatform;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.profile.ProfileDao;
import java.util.List;
import java.util.UUID;
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
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class ProfileConnectedTest {
  private static final Logger logger = LoggerFactory.getLogger(ProfileConnectedTest.class);

  @Autowired private ProfileDao profileDao;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ConfigurationService configService;
  @Autowired private ApplicationConfiguration applicationConfiguration;

  @MockBean private IamProviderInterface samService;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
  }

  @After
  public void tearDown() throws Exception {
    connectedOperations.teardown();
    configService.reset();
  }

  @Test
  public void testGoogleIsDefault() throws Exception {
    var requestModel =
        ProfileFixtures.randomBillingProfileRequest()
            .billingAccountId(testConfig.getGoogleBillingAccountId())
            .cloudPlatform(null);
    var profile = connectedOperations.createProfile(requestModel);
    var retrievedProfile = connectedOperations.getProfileById(profile.getId());
    assertThat(
        "GCP is the default cloud provider",
        retrievedProfile.getCloudPlatform(),
        equalTo(CloudPlatform.GCP));
  }

  @Test
  public void testAzureBillingProfile() throws Exception {
    if (!ValidationUtils.isValidEmail(applicationConfiguration.getUserEmail())) {
      logger.info("Skipping test since default user was not set");
    }
    var tenant = testConfig.getTargetTenantId();
    var subscription = testConfig.getTargetSubscriptionId();
    var resourceGroup = testConfig.getTargetResourceGroupName();
    var applicationName = testConfig.getTargetApplicationName();
    var requestModel =
        ProfileFixtures.randomizeAzureBillingProfileRequest()
            .billingAccountId("")
            .cloudPlatform(CloudPlatform.AZURE)
            .tenantId(tenant)
            .subscriptionId(subscription)
            .resourceGroupName(resourceGroup)
            .applicationDeploymentName(applicationName);

    var profile = connectedOperations.createProfile(requestModel);

    var retrievedProfile = connectedOperations.getProfileById(profile.getId());

    assertThat(
        "The response has the correct cloudPlatform",
        retrievedProfile.getCloudPlatform(),
        equalTo(CloudPlatform.AZURE));

    assertThat(
        "Azure billing profile does not have a google billingAccountId",
        retrievedProfile.getBillingAccountId(),
        is(emptyOrNullString()));

    assertThat(
        "Azure billing profile has tenant, subscription, resourceGroup, and applicationName",
        List.of(
            retrievedProfile.getTenantId(),
            retrievedProfile.getSubscriptionId(),
            retrievedProfile.getResourceGroupName(),
            retrievedProfile.getApplicationDeploymentName()),
        contains(tenant, subscription, resourceGroup, applicationName));
  }

  @Test
  public void testGoogleInvalidAzureParams() throws Exception {
    var gcpRequestModel =
        ProfileFixtures.randomBillingProfileRequest()
            .cloudPlatform(CloudPlatform.GCP)
            .tenantId(UUID.randomUUID())
            .subscriptionId(UUID.randomUUID())
            .resourceGroupName("resourceGroupName");

    var defaultRequestModel =
        ProfileFixtures.randomBillingProfileRequest()
            .tenantId(UUID.randomUUID())
            .subscriptionId(UUID.randomUUID())
            .resourceGroupName("resourceGroupName");

    for (var requestModel : List.of(gcpRequestModel, defaultRequestModel)) {
      var errorModel =
          connectedOperations.createProfileExpectError(requestModel, HttpStatus.BAD_REQUEST);

      assertThat("There are 3 errors returned", errorModel.getErrorDetail(), iterableWithSize(3));

      assertThat(
          "GCP request returns tenantId error if supplied",
          errorModel.getErrorDetail(),
          hasItems(startsWith("tenantId")));
      assertThat(
          "GCP request returns subscriptionId error if supplied",
          errorModel.getErrorDetail(),
          hasItems(startsWith("subscriptionId")));
      assertThat(
          "GCP request returns resourceGroupName error if supplied",
          errorModel.getErrorDetail(),
          hasItems(startsWith("resourceGroupName")));
    }
  }

  @Test
  public void testAzureInvalidParams() throws Exception {
    var azureRequestModel =
        ProfileFixtures.randomBillingProfileRequest().cloudPlatform(CloudPlatform.AZURE);
    var invalidParams =
        connectedOperations.createProfileExpectError(azureRequestModel, HttpStatus.BAD_REQUEST);

    assertThat("There are 4 errors returned", invalidParams.getErrorDetail(), iterableWithSize(4));

    assertThat(
        "Azure request returns tenantId error if not supplied",
        invalidParams.getErrorDetail(),
        hasItems(containsString("UUID `tenantId`")));
    assertThat(
        "Azure request returns subscriptionId error if not supplied",
        invalidParams.getErrorDetail(),
        hasItems(containsString("UUID `subscriptionId`")));
    assertThat(
        "Azure request returns resourceGroupName error if not supplied",
        invalidParams.getErrorDetail(),
        hasItems(containsString("non-empty resourceGroupName")));
    assertThat(
        "Azure request returns google billingAccountId error if supplied",
        invalidParams.getErrorDetail(),
        hasItems(containsString("billing account id")));
  }
}
