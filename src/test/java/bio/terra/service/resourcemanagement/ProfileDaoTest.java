package bio.terra.service.resourcemanagement;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
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
    private UUID makeProfile() {
        BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
        BillingProfileModel billingProfileModel = profileDao.createBillingProfile(profileRequest, "me@me.me");
        UUID profileId = UUID.fromString(billingProfileModel.getId());
        profileIds.add(profileId);
        return profileId;
    }

    @Test(expected = ProfileNotFoundException.class)
    public void profileDeleteTest() {
        UUID profileId = makeProfile();
        boolean deleted = profileDao.deleteBillingProfileById(profileId);
        assertThat("able to delete", deleted, equalTo(true));
        profileDao.getBillingProfileById(profileId);
    }

    @Test
    public void profileEnumerateTest() throws Exception {
        Map<UUID, String> profileIdToAccountId = new HashMap<>();
        List<UUID> accessibleProfileId = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            UUID enumProfileId = makeProfile();
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
