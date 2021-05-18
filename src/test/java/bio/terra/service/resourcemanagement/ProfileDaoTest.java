package bio.terra.service.resourcemanagement;

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class ProfileDaoTest {

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private ProfileService profileService;

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
        BillingProfileModel billingProfileModel = profileDao.createBillingProfile(profileRequest, "me@me.me");
        UUID profileId = UUID.fromString(billingProfileModel.getId());
        profileIds.add(profileId);
        return billingProfileModel;
    }

    @Test
    public void profileCloudProvidersTest() throws Exception {
        BillingProfileModel googleBillingProfile = makeProfile();
        String tenant = UUID.randomUUID().toString();
        String subscription = UUID.randomUUID().toString();
        String resourceGroup = UUID.randomUUID().toString();
        BillingProfileRequestModel azureBillingProfileRequest = ProfileFixtures.randomBillingProfileRequest()
            .cloudPlatform(CloudPlatform.AZURE)
            .tenant(tenant)
            .subscription(subscription)
            .resourceGroup(resourceGroup);
        BillingProfileModel azureBillingProfile =
            profileDao.createBillingProfile(azureBillingProfileRequest, "me@me.me");
        UUID azureProfileId = UUID.fromString(azureBillingProfile.getId());
        profileIds.add(azureProfileId);

        BillingProfileModel retrievedGoogleBillingProfile =
            profileDao.getBillingProfileById(UUID.fromString(googleBillingProfile.getId()));
        BillingProfileModel retrievedAzureBillingProfile =
            profileDao.getBillingProfileById(UUID.fromString(azureBillingProfile.getId()));

        assertThat("GCP is the default cloud platform",
            retrievedGoogleBillingProfile.getCloudPlatform(),
            equalTo(CloudPlatform.GCP));

        assertThat("GCP billing profile does not have tenant, subscription, and resourceGroup",
            new String[]{retrievedGoogleBillingProfile.getTenant(), retrievedGoogleBillingProfile.getSubscription(),
                retrievedGoogleBillingProfile.getResourceGroup()},
            equalTo(new String[]{null, null, null}));

        assertThat("Azure cloud platform is correctly stored",
            retrievedAzureBillingProfile.getCloudPlatform(),
            equalTo(CloudPlatform.AZURE));

        assertThat("Azure billing profile has tenant, subscription, and resourceGroup",
            new String[]{retrievedAzureBillingProfile.getTenant(), retrievedAzureBillingProfile.getSubscription(),
                retrievedAzureBillingProfile.getResourceGroup()},
            equalTo(new String[]{tenant, subscription, resourceGroup}));

    }

    @Test(expected = ProfileNotFoundException.class)
    public void profileDeleteTest() {
        UUID profileId = UUID.fromString(makeProfile().getId());
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
        String testBA = profileDao.getBillingProfileById(UUID.fromString(profile.getId())).getBillingAccountId();
        assertThat("Billing account should be equal.", testBA, equalTo(oldBillingAccount));

        // test the update function
        BillingProfileUpdateModel updateModel = new BillingProfileUpdateModel()
            .id(profile.getId())
            .billingAccountId(newBillingAccount)
            .description("updated");
        BillingProfileModel newProfile = profileDao.updateBillingProfileById(updateModel);

        assertThat("Billing profile should be updated.", newProfile.getBillingAccountId(),
            equalTo(newBillingAccount));
        assertThat("Description should be updated", newProfile.getDescription(),
            containsString("updated"));
    }

    @Test(expected = ProfileNotFoundException.class)
    public void updateNonExistentProfile() {
        BillingProfileUpdateModel updateModel = new BillingProfileUpdateModel()
            .id(UUID.randomUUID().toString())
            .billingAccountId(ProfileFixtures.randomBillingAccountId())
            .description("random");
        profileDao.updateBillingProfileById(updateModel);
    }

    @Test
    public void profileEnumerateTest() throws Exception {
        Map<UUID, String> profileIdToAccountId = new HashMap<>();
        List<UUID> accessibleProfileId = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            UUID enumProfileId = UUID.fromString(makeProfile().getId());
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

    private void testOneEnumerateRange(Map<UUID, String> profileIdToAccountId,
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
            UUID profileId = UUID.fromString(profile.getId());
            if (profileIdToAccountId.containsKey(profileId)) {
                assertThat("account id matches profile id",
                    profile.getBillingAccountId(),
                    equalTo(profileIdToAccountId.get(profileId)));
            }
        }
    }
}
