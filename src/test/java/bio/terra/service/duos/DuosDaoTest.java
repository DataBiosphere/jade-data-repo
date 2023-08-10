package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.model.DuosFirecloudGroupModel;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class DuosDaoTest {
  @Autowired private NamedParameterJdbcTemplate jdbcTemplate;
  private DuosDao duosDao;

  private static final String TDR_SERVICE_ACCOUNT_EMAIL = "tdr-sa@a.com";
  private static final String DUOS_ID = "DUOS-ID";

  private List<UUID> duosFirecloudGroupIds;
  private String firecloudGroupName;
  private String firecloudGroupEmail;
  private DuosFirecloudGroupModel toInsert;

  @Before
  public void before() {
    duosDao = new DuosDao(jdbcTemplate, TDR_SERVICE_ACCOUNT_EMAIL);

    duosFirecloudGroupIds = new ArrayList<>();
    firecloudGroupName = String.format("%s_users", DUOS_ID);
    firecloudGroupEmail = String.format("%s@dev.test.firecloud.org", firecloudGroupName);
    toInsert =
        new DuosFirecloudGroupModel()
            .duosId(DUOS_ID)
            .firecloudGroupName(firecloudGroupName)
            .firecloudGroupEmail(firecloudGroupEmail);
  }

  @After
  public void after() {
    for (UUID duosFirecloudGroupId : duosFirecloudGroupIds) {
      assertTrue(
          "DUOS Firecloud group record %s was deleted".formatted(duosFirecloudGroupId),
          duosDao.deleteFirecloudGroup(duosFirecloudGroupId));
    }
  }

  @Test
  public void testCoverageMethod() {
    assertThat(duosDao.testMethodForCoverage(), equalTo(1));
  }

  @Test
  public void testRetrieveFirecloudGroupBeforeInsert() {
    assertNull(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID));
    assertThat(duosDao.retrieveFirecloudGroups(), empty());
    assertThat(duosDao.retrieveFirecloudGroups(List.of()), empty());
  }

  @Test
  public void testDeleteNonExistentFirecloudGroup() {
    assertFalse(duosDao.deleteFirecloudGroup(null));
    assertFalse(duosDao.deleteFirecloudGroup(UUID.randomUUID()));
  }

  @Test
  public void testInsertAndRetrieveFirecloudGroup() {
    DuosFirecloudGroupModel retrieved = duosDao.insertAndRetrieveFirecloudGroup(toInsert);
    UUID id = retrieved.getId();
    duosFirecloudGroupIds.add(id);

    verifyRetrievedFirecloudGroupContents(retrieved);
  }

  @Test
  public void testInsertAndRetrieveFirecloudGroupThrowsOnDuplicateDuosId() {
    UUID id = duosDao.insertAndRetrieveFirecloudGroup(toInsert).getId();
    duosFirecloudGroupIds.add(id);

    assertThrows(
        DuplicateKeyException.class,
        () ->
            duosDao.insertAndRetrieveFirecloudGroup(
                new DuosFirecloudGroupModel()
                    .duosId(DUOS_ID)
                    .firecloudGroupName("another-fc-group-name")
                    .firecloudGroupEmail("another-fc-group-name@dev.test.firecloud.org")));
  }

  @Test
  public void testUpdateFirecloudGroupLastSyncedDateForNonexistentRow() {
    assertFalse(duosDao.updateFirecloudGroupLastSyncedDate(UUID.randomUUID(), Instant.now()));
  }

  @Test
  public void testUpdateFirecloudGroupLastSyncedDate() {
    UUID id = duosDao.insertAndRetrieveFirecloudGroup(toInsert).getId();
    duosFirecloudGroupIds.add(id);
    // When reading back an Instant written to Postgres, the precision can differ.
    // Parsing an Instant from a fixed lower precision string representation allows us
    // to verify expectations when reading back the record.
    Instant lastSyncedDate = Instant.parse("2022-11-17T00:00:00.00Z");

    assertTrue(duosDao.updateFirecloudGroupLastSyncedDate(id, lastSyncedDate));

    DuosFirecloudGroupModel updated = duosDao.retrieveFirecloudGroup(id);
    verifyRetrievedFirecloudGroupContents(updated, lastSyncedDate);
  }

  @Test
  public void testUpdateFirecloudGroupsLastSyncedDate() {
    for (int i = 0; i < 3; i++) {
      UUID id = duosDao.insertAndRetrieveFirecloudGroup(toInsert.duosId(DUOS_ID + i)).getId();
      duosFirecloudGroupIds.add(id);
    }

    Instant lastSyncedDate = Instant.parse("2022-11-17T00:00:00.00Z");
    assertThat(
        "No records are updated when no record IDs are passed in",
        duosDao.updateFirecloudGroupsLastSyncedDate(List.of(), lastSyncedDate),
        equalTo(0));
    assertThat(
        "All records have their last synced date updated",
        duosDao.updateFirecloudGroupsLastSyncedDate(duosFirecloudGroupIds, lastSyncedDate),
        equalTo(duosFirecloudGroupIds.size()));

    duosDao
        .retrieveFirecloudGroups(duosFirecloudGroupIds)
        .forEach(updated -> verifyRetrievedFirecloudGroupContents(updated, lastSyncedDate));
  }

  private void verifyRetrievedFirecloudGroupContents(
      DuosFirecloudGroupModel retrieved, Instant lastSyncedDate) {
    assertThat(retrieved, notNullValue());
    assertThat(retrieved.getDuosId(), startsWith(DUOS_ID));
    assertThat(retrieved.getFirecloudGroupName(), equalTo(firecloudGroupName));
    assertThat(retrieved.getFirecloudGroupEmail(), equalTo(firecloudGroupEmail));
    assertThat(retrieved.getCreatedBy(), equalTo(TDR_SERVICE_ACCOUNT_EMAIL));
    assertThat(retrieved.getCreated(), notNullValue());

    Instant actualLastSynced =
        Optional.ofNullable(retrieved.getLastSynced()).map(Instant::parse).orElse(null);
    assertThat(actualLastSynced, equalTo(lastSyncedDate));
  }

  private void verifyRetrievedFirecloudGroupContents(DuosFirecloudGroupModel retrieved) {
    verifyRetrievedFirecloudGroupContents(retrieved, null);
  }
}
