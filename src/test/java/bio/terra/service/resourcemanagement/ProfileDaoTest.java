package bio.terra.service.resourcemanagement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class ProfileDaoTest {

  @Autowired private ProfileDao profileDao;

  @Autowired private ProfileService profileService;

  private ArrayList<UUID> profileIds;

  @Before
  public void setup() throws Exception {
    profileIds = new ArrayList<>();
  }

  @After
  public void teardown() throws Exception {
    for (UUID profileId : profileIds) {
      profileDao.deleteBillingProfileById(profileId);
    }
  }

  // keeps track of the profiles that are made so they can be cleaned up
  private BillingProfileModel makeProfile() {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    BillingProfileModel billingProfileModel =
        profileDao.createBillingProfile(profileRequest, "me@me.me");
    assertRequestMatchesResult(profileRequest, billingProfileModel);
    UUID profileId = billingProfileModel.getId();
    profileIds.add(profileId);
    return billingProfileModel;
  }

  @Test
  public void profileCloudProvidersTest() throws Exception {
    var googleBillingProfile = makeProfile();
    var tenant = UUID.randomUUID();
    var subscription = UUID.randomUUID();
    var resourceGroup = "resourceGroupName";
    var applicationName = "applicationName";
    var azureBillingProfileRequest =
        ProfileFixtures.randomBillingProfileRequest()
            .cloudPlatform(CloudPlatform.AZURE)
            .tenantId(tenant)
            .subscriptionId(subscription)
            .resourceGroupName(resourceGroup)
            .applicationDeploymentName(applicationName);
    var azureBillingProfile =
        profileDao.createBillingProfile(azureBillingProfileRequest, "me@me.me");
    assertRequestMatchesResult(azureBillingProfileRequest, azureBillingProfile);
    var azureProfileId = azureBillingProfile.getId();
    profileIds.add(azureProfileId);

    var retrievedGoogleBillingProfile =
        profileDao.getBillingProfileById(googleBillingProfile.getId());
    var retrievedAzureBillingProfile =
        profileDao.getBillingProfileById(azureBillingProfile.getId());

    assertThat(
        "GCP is the default cloud platform",
        retrievedGoogleBillingProfile.getCloudPlatform(),
        equalTo(CloudPlatform.GCP));

    assertThat(
        "GCP billing profile does not have tenant, subscription, resourceGroup, or applicationName",
        Arrays.asList(
            Optional.ofNullable(retrievedGoogleBillingProfile.getTenantId())
                .map(UUID::toString)
                .orElse(""),
            Optional.ofNullable(retrievedGoogleBillingProfile.getSubscriptionId())
                .map(UUID::toString)
                .orElse(""),
            retrievedGoogleBillingProfile.getResourceGroupName(),
            retrievedGoogleBillingProfile.getApplicationDeploymentName()),
        everyItem(is(emptyOrNullString())));

    assertThat(
        "Azure cloud platform is correctly stored",
        retrievedAzureBillingProfile.getCloudPlatform(),
        equalTo(CloudPlatform.AZURE));

    assertThat(
        "Azure billing profile has tenant, subscription, resourceGroup, and applicationName",
        List.of(
            retrievedAzureBillingProfile.getTenantId(),
            retrievedAzureBillingProfile.getSubscriptionId(),
            retrievedAzureBillingProfile.getResourceGroupName(),
            retrievedAzureBillingProfile.getApplicationDeploymentName()),
        contains(tenant, subscription, resourceGroup, applicationName));
  }

  @Test(expected = ProfileNotFoundException.class)
  public void profileDeleteTest() {
    UUID profileId = makeProfile().getId();
    boolean deleted = profileDao.deleteBillingProfileById(profileId);
    assertThat("able to delete", deleted, equalTo(true));
    profileDao.getBillingProfileById(profileId);
  }

  @Test
  public void profileUpdate() {
    BillingProfileModel profile = makeProfile();

    // Start with old Billing account, then set to newBillingAccount
    String oldBillingAccount = profile.getBillingAccountId();
    String newBillingAccount = ProfileFixtures.randomBillingAccountId();

    // Check existing state: Test billing profile set as expected
    String testBA = profileDao.getBillingProfileById(profile.getId()).getBillingAccountId();
    assertThat("Billing account should be equal.", testBA, equalTo(oldBillingAccount));

    // test the update function
    BillingProfileUpdateModel updateModel =
        new BillingProfileUpdateModel()
            .id(profile.getId())
            .billingAccountId(newBillingAccount)
            .description("updated");
    BillingProfileModel newProfile = profileDao.updateBillingProfileById(updateModel);

    assertThat(
        "Billing profile should be updated.",
        newProfile.getBillingAccountId(),
        equalTo(newBillingAccount));
    assertThat(
        "Description should be updated", newProfile.getDescription(), containsString("updated"));
  }

  @Test(expected = ProfileNotFoundException.class)
  public void updateNonExistentProfile() {
    BillingProfileUpdateModel updateModel =
        new BillingProfileUpdateModel()
            .id(UUID.randomUUID())
            .billingAccountId(ProfileFixtures.randomBillingAccountId())
            .description("random");
    profileDao.updateBillingProfileById(updateModel);
  }

  @Test
  public void profileEnumerateTest() throws Exception {
    Map<UUID, String> profileIdToAccountId = new HashMap<>();
    List<UUID> accessibleProfileId = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      UUID enumProfileId = makeProfile().getId();
      BillingProfileModel enumProfile = profileDao.getBillingProfileById(enumProfileId);
      profileIdToAccountId.put(enumProfileId, enumProfile.getBillingAccountId());
      accessibleProfileId.add(enumProfileId);
    }

    EnumerateBillingProfileModel profileEnumeration =
        profileDao.enumerateBillingProfiles(0, 1, accessibleProfileId);
    int total = profileEnumeration.getTotal();
    testOneEnumerateRange(profileIdToAccountId, accessibleProfileId, 0, total);
    testOneEnumerateRange(profileIdToAccountId, accessibleProfileId, 0, total + 10);
    testOneEnumerateRange(profileIdToAccountId, accessibleProfileId, 1, total - 3);
    testOneEnumerateRange(profileIdToAccountId, accessibleProfileId, total - 3, total + 3);
    testOneEnumerateRange(profileIdToAccountId, accessibleProfileId, total, 1);
  }

  private void testOneEnumerateRange(
      Map<UUID, String> profileIdToAccountId,
      List<UUID> accessibleProfileId,
      int offset,
      int limit) {
    EnumerateBillingProfileModel profileMetadataEnumeration =
        profileDao.enumerateBillingProfiles(offset, limit, accessibleProfileId);

    // We expect the snapshots to be returned in their created order
    List<BillingProfileModel> profiles = profileMetadataEnumeration.getItems();
    int total = profileMetadataEnumeration.getTotal();
    int expected = Math.min(total - offset, limit);
    assertThat("received expected number of profiles", profiles.size(), equalTo(expected));

    for (BillingProfileModel profile : profiles) {
      UUID profileId = profile.getId();
      if (profileIdToAccountId.containsKey(profileId)) {
        assertThat(
            "account id matches profile id",
            profile.getBillingAccountId(),
            equalTo(profileIdToAccountId.get(profileId)));
      }
    }
  }

  private void assertRequestMatchesResult(
      BillingProfileRequestModel request, BillingProfileModel result) {
    assertThat("Names match", result.getProfileName(), equalTo(request.getProfileName()));
    assertThat("Descriptions match", result.getDescription(), equalTo(request.getDescription()));
    assertThat("Billers match", result.getBiller(), equalTo(request.getBiller()));
    assertThat(
        "Billing cloud platforms match",
        result.getCloudPlatform(),
        equalTo(request.getCloudPlatform()));
    assertThat(
        "Billing accounts match",
        result.getBillingAccountId(),
        equalTo(request.getBillingAccountId()));
    assertThat("Tenants match", result.getTenantId(), equalTo(request.getTenantId()));
    assertThat(
        "Subscriptions match", result.getSubscriptionId(), equalTo(request.getSubscriptionId()));
    assertThat(
        "Resource groups match",
        result.getResourceGroupName(),
        equalTo(request.getResourceGroupName()));
    assertThat(
        "Application deployments match",
        result.getApplicationDeploymentName(),
        equalTo(request.getApplicationDeploymentName()));
  }
}
