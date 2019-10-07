package bio.terra.service.resourcemanagement;

import bio.terra.category.Unit;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.service.resourcemanagement.exception.ProfileNotFoundException;
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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
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

    private BillingProfile billingProfile;
    private ArrayList<UUID> profileIds;

    @Before
    public void setup() throws Exception {
        billingProfile = ProfileFixtures.randomBillingProfile();
        profileIds = new ArrayList<>();
    }

    @After
    public void teardown() throws Exception {
        for (UUID profileId : profileIds) {
            profileDao.deleteBillingProfileById(profileId);
        }
    }

    // keeps track of the profiles that are made so they can be cleaned up
    private UUID makeProfile(BillingProfile profile) {
        UUID profileId = profileDao.createBillingProfile(profile);
        profileIds.add(profileId);
        return profileId;
    }

    @Test
    public void happyProfileInOutTest() throws Exception {
        UUID profileId = makeProfile(billingProfile);
        BillingProfile fromDB = profileDao.getBillingProfileById(profileId);
        List<BillingProfile> byBillingAccountId =
            profileDao.getBillingProfilesByAccount(billingProfile.getBillingAccountId());

        assertThat("profile name set correctly",
                fromDB.getName(),
                equalTo(billingProfile.getName()));

        assertThat("profile billing account id set correctly",
                fromDB.getBillingAccountId(),
                equalTo(billingProfile.getBillingAccountId()));

        assertThat("profile biller set correctly",
                fromDB.getBiller(),
                equalTo(billingProfile.getBiller()));

        assertThat("able to lookup by account id",
            byBillingAccountId.stream().map(BillingProfile::getId).collect(Collectors.toList()),
            contains(equalTo(profileId)));
    }

    @Test(expected = ProfileNotFoundException.class)
    public void profileDeleteTest() {
        UUID profileId = makeProfile(billingProfile);
        boolean deleted = profileDao.deleteBillingProfileById(profileId);
        assertThat("able to delete", deleted, equalTo(true));
        profileDao.getBillingProfileById(profileId);
    }

    @Test
    public void profileEnumerateTest() throws Exception {
        Map<UUID, String> profileIdToAccountId = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            BillingProfile enumProfile = ProfileFixtures.randomBillingProfile();
            UUID enumProfileId = makeProfile(enumProfile);
            profileIdToAccountId.put(enumProfileId, enumProfile.getBillingAccountId());
        }

        MetadataEnumeration<BillingProfile> profileEnumeration = profileDao.enumerateBillingProfiles(0, 1);
        int total = profileEnumeration.getTotal();
        testOneEnumerateRange(profileIdToAccountId, 0, total);
        testOneEnumerateRange(profileIdToAccountId, 0, total + 10);
        testOneEnumerateRange(profileIdToAccountId, 1, total - 3);
        testOneEnumerateRange(profileIdToAccountId, total - 3, total + 3);
        testOneEnumerateRange(profileIdToAccountId, total, 1);
    }

    private void testOneEnumerateRange(Map<UUID, String> profileIdToAccountId, int offset, int limit) {
        MetadataEnumeration<BillingProfile> profileMetadataEnumeration =
            profileDao.enumerateBillingProfiles(offset, limit);

        // We expect the snapshots to be returned in their created order
        List<BillingProfile> profiles = profileMetadataEnumeration.getItems();
        int total = profileMetadataEnumeration.getTotal();
        int expected = Math.min(total - offset, limit);
        assertThat("received expected number of profiles", profiles.size(), equalTo(expected));

        for (BillingProfile profile : profiles) {
            UUID profileId = profile.getId();
            if (profileIdToAccountId.containsKey(profileId)) {
                assertThat("account id matches profile id",
                    profile.getBillingAccountId(),
                    equalTo(profileIdToAccountId.get(profileId)));
            }
        }
    }
}
