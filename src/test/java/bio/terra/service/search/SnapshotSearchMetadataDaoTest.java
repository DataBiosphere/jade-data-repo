package bio.terra.service.search;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Connected.class)
public class SnapshotSearchMetadataDaoTest {

  @Autowired private GoogleResourceDao resourceDao;
  @Autowired private ProfileDao profileDao;
  @Autowired private SnapshotDao snapshotDao;
  @Autowired private SnapshotSearchMetadataDao snapshotSearchDao;

  private UUID profileId;
  private UUID projectId;
  private List<UUID> snapshotIds;

  @Before
  public void before() throws Exception {
    BillingProfileModel billingProfile =
        profileDao.createBillingProfile(ProfileFixtures.randomBillingProfileRequest(), "hi@hi.hi");
    profileId = billingProfile.getId();

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    projectId = resourceDao.createProject(projectResource);

    String flightId = UUID.randomUUID().toString();

    snapshotIds =
        Stream.generate(UUID::randomUUID)
            .limit(3)
            .peek(
                uuid -> {
                  var snapshot =
                      new Snapshot()
                          .id(uuid)
                          .name("snap-" + uuid)
                          .profileId(profileId)
                          .projectResourceId(projectId);
                  snapshotDao.createAndLock(snapshot, flightId);
                })
            .collect(Collectors.toList());
  }

  @After
  public void after() throws Exception {
    snapshotIds.forEach(snapshotDao::delete);
    resourceDao.deleteProject(projectId);
    profileDao.deleteBillingProfileById(profileId);
  }

  // Test invalid foreign key constraint on snapshot ID.
  @Test(expected = DataIntegrityViolationException.class)
  public void testInvalidSnapshotId() {
    snapshotSearchDao.putMetadata(new Snapshot().id(UUID.randomUUID()), "{}");
  }

  // Test invalid JSON data.
  @Test(expected = DataIntegrityViolationException.class)
  public void testInvalidJsonData() {
    snapshotSearchDao.putMetadata(new Snapshot().id(snapshotIds.get(0)), "not json!");
  }

  @Test
  public void testPutGetDelete() {
    // Test get with no data.
    assertThat(snapshotSearchDao.getMetadata(snapshotIds), is(anEmptyMap()));

    // Test put/get with data.
    var snapshotId = snapshotIds.get(0);
    var snapshot = new Snapshot().id(snapshotId);
    // The whitespace here is important as JSONB will reformat the JSON data.
    var data = "{\"name\": \"test\"}";
    snapshotSearchDao.putMetadata(snapshot, data);
    assertThat(snapshotSearchDao.getMetadata(snapshotIds), is(Map.of(snapshotId, data)));

    // Test data update.
    var data2 = "{\"another\": \"something\"}";
    snapshotSearchDao.putMetadata(snapshot, data2);
    assertThat(snapshotSearchDao.getMetadata(snapshotIds), is(Map.of(snapshotId, data2)));

    // Test delete.
    snapshotSearchDao.deleteMetadata(snapshot);
    assertThat(snapshotSearchDao.getMetadata(snapshotIds), is(anEmptyMap()));
  }
}
