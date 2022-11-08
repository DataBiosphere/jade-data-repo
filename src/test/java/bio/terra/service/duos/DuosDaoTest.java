package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.model.DuosFirecloudGroupModel;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class DuosDaoTest {

  @Autowired private DuosDao duosDao;

  private UUID duosFirecloudGroupId;
  private String duosId;
  private String firecloudGroupName;
  private String firecloudGroupEmail;
  private String tdrServiceAccountEmail;
  private DuosFirecloudGroupModel toInsert;

  @Before
  public void before() {
    duosId = UUID.randomUUID().toString();
    firecloudGroupName = String.format("%s_users", duosId);
    firecloudGroupEmail = String.format("%s@dev.test.firecloud.org", firecloudGroupName);
    tdrServiceAccountEmail = duosDao.getTdrServiceAccountEmail();
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

  private void verifyRetrievedFirecloudGroupContents(DuosFirecloudGroupModel retrieved) {
    assertThat(retrieved, notNullValue());
    assertThat(retrieved.getId(), equalTo(duosFirecloudGroupId));
    assertThat(retrieved.getFirecloudGroupName(), equalTo(firecloudGroupName));
    assertThat(retrieved.getFirecloudGroupEmail(), equalTo(firecloudGroupEmail));
    assertThat(retrieved.getCreatedBy(), equalTo(tdrServiceAccountEmail));
    assertThat(retrieved.getCreated(), notNullValue());
    assertThat(retrieved.getLastSynced(), nullValue());
  }
}
