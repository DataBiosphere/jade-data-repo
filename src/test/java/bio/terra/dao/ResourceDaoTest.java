package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.fixtures.ResourceFixtures;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.Column;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.DatasetSummary;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.service.DatasetService;
import bio.terra.service.ResourceService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class ResourceDaoTest {

    @Autowired
    private ResourceDao resourceDao;

    @Autowired
    private ResourceService resourceService;

    private BillingProfile billingProfile;
    private ArrayList<UUID> profileIds;

    @Before
    public void setup() throws Exception {
        billingProfile = ResourceFixtures.randomBillingProfile();
        profileIds = new ArrayList<>();
    }

    @After
    public void teardown() throws Exception {
        for (UUID profileId : profileIds) {
            resourceDao.deleteBillingProfileById(profileId);
        }
    }

    // keeps track of the profiles that are made so they can be cleaned up
    private UUID makeProfile(BillingProfile profile) {
        UUID profileId = resourceDao.createBillingProfile(profile);
        profileIds.add(profileId);
        return profileId;
    }

    @Test
    public void happyProfileInOutTest() throws Exception {
        UUID profileId = makeProfile(billingProfile);
        BillingProfile fromDB = resourceDao.getBillingProfileById(profileId);

        assertThat("profile name set correctly",
                fromDB.getName(),
                equalTo(billingProfile.getName()));

        assertThat("profile billing account id set correctly",
                fromDB.getBillingAccountId(),
                equalTo(billingProfile.getBillingAccountId()));

        assertThat("profile biller set correctly",
                fromDB.getBiller(),
                equalTo(billingProfile.getBiller()));
    }

    @Test
    public void profileEnumerateTest() throws Exception {
        Map<UUID, String> profileIdToAccountId = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            BillingProfile enumProfile = ResourceFixtures.randomBillingProfile();
            UUID enumProfileId = makeProfile(enumProfile);
            profileIdToAccountId.put(enumProfileId, enumProfile.getBillingAccountId());
        }

        MetadataEnumeration<BillingProfile> profileEnumeration = resourceDao.enumerateBillingProfiles(0, 1);
        int total = profileEnumeration.getTotal();
        testOneEnumerateRange(profileIdToAccountId, 0, total);
        testOneEnumerateRange(profileIdToAccountId, 0, total + 10);
        testOneEnumerateRange(profileIdToAccountId, 1, total - 3);
        testOneEnumerateRange(profileIdToAccountId, total - 3, total + 3);
        testOneEnumerateRange(profileIdToAccountId, total, 1);
    }

    private void testOneEnumerateRange(Map<UUID, String> profileIdToAccountId, int offset, int limit) {
        MetadataEnumeration<BillingProfile> profileMetadataEnumeration =
            resourceDao.enumerateBillingProfiles(offset, limit);

        // We expect the datasets to be returned in their created order
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
