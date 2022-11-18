package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.model.DuosFirecloudGroupModel;
import java.time.Instant;
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

  private UUID duosFirecloudGroupId;
  private String duosId;
  private String firecloudGroupName;
  private String firecloudGroupEmail;
  private DuosFirecloudGroupModel toInsert;

  @Before
  public void before() {
    duosDao = new DuosDao(jdbcTemplate, TDR_SERVICE_ACCOUNT_EMAIL);

    duosId = UUID.randomUUID().toString();
    firecloudGroupName = String.format("%s_users", duosId);
    firecloudGroupEmail = String.format("%s@dev.test.firecloud.org", firecloudGroupName);
    toInsert =
        new DuosFirecloudGroupModel()
            .duosId(duosId)
            .firecloudGroupName(firecloudGroupName)
            .firecloudGroupEmail(firecloudGroupEmail);
  }

  @After
  public void after() {
    if (duosFirecloudGroupId != null) {
      assertTrue(duosDao.deleteFirecloudGroup(duosFirecloudGroupId));
    }
  }

  @Test
  public void testRetrieveFirecloudGroupBeforeInsert() {
    DuosFirecloudGroupModel retrieveBeforeInsert = duosDao.retrieveFirecloudGroupByDuosId(duosId);
    assertNull(retrieveBeforeInsert);
  }

  @Test
  public void testDeleteNonExistentFirecloudGroup() {
    assertFalse(duosDao.deleteFirecloudGroup(null));
    assertFalse(duosDao.deleteFirecloudGroup(UUID.randomUUID()));
  }

  @Test
  public void testInsertThenRetrieveFirecloudGroup() {
    duosFirecloudGroupId = duosDao.insertFirecloudGroup(toInsert);

    DuosFirecloudGroupModel retrievedByDuosId = duosDao.retrieveFirecloudGroupByDuosId(duosId);
    verifyRetrievedFirecloudGroupContents(retrievedByDuosId);

    DuosFirecloudGroupModel retrievedById = duosDao.retrieveFirecloudGroup(duosFirecloudGroupId);
    verifyRetrievedFirecloudGroupContents(retrievedById);
  }

  @Test
  public void testInsertAndRetrieveFirecloudGroup() {
    DuosFirecloudGroupModel retrieved = duosDao.insertAndRetrieveFirecloudGroup(toInsert);
    duosFirecloudGroupId = retrieved.getId();

    verifyRetrievedFirecloudGroupContents(retrieved);
  }

  @Test
  public void testInsertFirecloudGroupThrowsOnDuplicateDuosId() {
    duosFirecloudGroupId = duosDao.insertFirecloudGroup(toInsert);

    assertThrows(
        DuplicateKeyException.class,
        () ->
            duosDao.insertFirecloudGroup(
                new DuosFirecloudGroupModel()
                    .duosId(duosId)
                    .firecloudGroupName("another-fc-group-name")
                    .firecloudGroupEmail("another-fc-group-name@dev.test.firecloud.org")));
  }

  @Test
  public void testUpdateFirecloudGroupLastSyncedDateForNonexistentRow() {
    assertFalse(duosDao.updateFirecloudGroupLastSyncedDate(UUID.randomUUID(), Instant.now()));
  }

  @Test
  public void testUpdateFirecloudGroupLastSyncedDate() {
    duosFirecloudGroupId = duosDao.insertFirecloudGroup(toInsert);
    Instant lastSyncedDate = Instant.parse("2022-11-17T00:00:00.00Z");

    assertTrue(duosDao.updateFirecloudGroupLastSyncedDate(duosFirecloudGroupId, lastSyncedDate));

    DuosFirecloudGroupModel retrieved = duosDao.retrieveFirecloudGroup(duosFirecloudGroupId);
    verifyRetrievedFirecloudGroupContents(retrieved, lastSyncedDate);
  }

  private void verifyRetrievedFirecloudGroupContents(
      DuosFirecloudGroupModel retrieved, Instant lastSyncedDate) {
    assertThat(retrieved, notNullValue());
    assertThat(retrieved.getId(), equalTo(duosFirecloudGroupId));
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
