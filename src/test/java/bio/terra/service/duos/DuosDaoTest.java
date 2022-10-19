package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.model.DuosFirecloudGroupModel;
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

  private static final String DUOS_ID = "DUOS-123456";
  private static final String FIRECLOUD_GROUP_NAME = String.format("%s_users", DUOS_ID);
  private static final String FIRECLOUD_GROUP_EMAIL =
      String.format("%s@dev.test.firecloud.org", FIRECLOUD_GROUP_NAME);

  private String tdrServiceAccountEmail;

  @Before
  public void before() {
    tdrServiceAccountEmail = duosDao.getTdrServiceAccountEmail();
  }

  @Test
  public void testInsertAndRetrieveFirecloudGroup() {
    DuosFirecloudGroupModel retrieveBeforeInsert = duosDao.retrieveFirecloudGroup(DUOS_ID);
    assertThat(retrieveBeforeInsert, nullValue());

    duosDao.insertFirecloudGroup(DUOS_ID, FIRECLOUD_GROUP_NAME, FIRECLOUD_GROUP_EMAIL);

    DuosFirecloudGroupModel retrieveAfterInsert = duosDao.retrieveFirecloudGroup(DUOS_ID);

    assertThat(retrieveAfterInsert, notNullValue());
    assertThat(retrieveAfterInsert.getFirecloudGroupName(), equalTo(FIRECLOUD_GROUP_NAME));
    assertThat(retrieveAfterInsert.getFirecloudGroupEmail(), equalTo(FIRECLOUD_GROUP_EMAIL));
    assertThat(retrieveAfterInsert.getCreatedBy(), equalTo(tdrServiceAccountEmail));
    assertThat(retrieveAfterInsert.getCreated(), notNullValue());
    assertThat(retrieveAfterInsert.getLastSynced(), nullValue());

    assertThrows(
        DuplicateKeyException.class,
        () ->
            duosDao.insertAndRetrieveFirecloudGroup(
                DUOS_ID, "different_firecloud_group_name", "different_firecloud_group_email"));
  }
}
