package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DuosServiceTest {

  @Mock private DuosDao duosDao;
  @Mock private IamService iamService;
  private DuosService duosService;

  private static final String DUOS_ID = "DUOS-123456";
  private static final String FIRECLOUD_GROUP_NAME = String.format("%s-users", DUOS_ID);
  private String firecloudGroupEmail;

  @Before
  public void before() {
    duosService = new DuosService(duosDao, iamService);
    firecloudGroupEmail = firecloudGroupEmail(FIRECLOUD_GROUP_NAME);
  }

  @Test
  public void testRetrieveFirecloudGroup() {
    assertTrue(duosService.retrieveFirecloudGroup(DUOS_ID).isEmpty());

    DuosFirecloudGroupModel group = new DuosFirecloudGroupModel().duosId(DUOS_ID);
    when(duosDao.retrieveFirecloudGroup(DUOS_ID)).thenReturn(group);
    Optional<DuosFirecloudGroupModel> retrieved = duosService.retrieveFirecloudGroup(DUOS_ID);

    assertTrue(retrieved.isPresent());
    assertThat(retrieved.get(), equalTo(group));
  }

  private String firecloudGroupEmail(String groupName) {
    return String.format("%s@dev.test.firecloud.org", groupName);
  }

  @Test
  public void testCreateFirecloudGroup() {
    when(iamService.createGroup(FIRECLOUD_GROUP_NAME)).thenReturn(firecloudGroupEmail);
    when(duosDao.insertFirecloudGroup(DUOS_ID, FIRECLOUD_GROUP_NAME, firecloudGroupEmail))
        .thenReturn(true);
    DuosFirecloudGroupModel group = new DuosFirecloudGroupModel().duosId(DUOS_ID);
    when(duosDao.retrieveFirecloudGroup(DUOS_ID)).thenReturn(group);

    assertThat(duosService.createFirecloudGroup(DUOS_ID), equalTo(group));
    verify(iamService, times(1)).createGroup(FIRECLOUD_GROUP_NAME);
    verify(iamService, never()).deleteGroup(FIRECLOUD_GROUP_NAME);
  }

  @Test
  public void testCreateFirecloudGroupWithNamingConflict() {
    String groupEmailNew = firecloudGroupEmail(FIRECLOUD_GROUP_NAME + "-new");

    // If we encounter a naming collision when trying to create a Firecloud group,
    // we don't know exactly what our backstop name will be as it will be suffixed by a new UUID,
    // but we know that it will start with our more readable group name.
    when(iamService.createGroup(startsWith(FIRECLOUD_GROUP_NAME))).thenReturn(groupEmailNew);
    IamConflictException iamConflictEx =
        new IamConflictException("Group already existed", List.of());
    doThrow(iamConflictEx).when(iamService).createGroup(FIRECLOUD_GROUP_NAME);

    when(duosDao.insertFirecloudGroup(
            eq(DUOS_ID), startsWith(FIRECLOUD_GROUP_NAME), eq(groupEmailNew)))
        .thenReturn(true);
    DuosFirecloudGroupModel group = new DuosFirecloudGroupModel().duosId(DUOS_ID);
    when(duosDao.retrieveFirecloudGroup(DUOS_ID)).thenReturn(group);

    assertThat(duosService.createFirecloudGroup(DUOS_ID), equalTo(group));
    // Our first creation attempt failed, so we tried to create again with our unique group name.
    verify(iamService, times(2)).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
    verify(duosDao, never())
        .insertFirecloudGroup(DUOS_ID, FIRECLOUD_GROUP_NAME, firecloudGroupEmail);
    verify(duosDao, times(1))
        .insertFirecloudGroup(eq(DUOS_ID), startsWith(FIRECLOUD_GROUP_NAME), eq(groupEmailNew));
  }

  @Test
  public void testCreateFirecloudGroupWithUnretriableCreationException() {
    IamForbiddenException iamForbiddenException =
        new IamForbiddenException("Unexpected SAM error", List.of());
    doThrow(iamForbiddenException).when(iamService).createGroup(FIRECLOUD_GROUP_NAME);

    assertThrows(IamForbiddenException.class, () -> duosService.createFirecloudGroup(DUOS_ID));
    verify(duosDao, never())
        .insertFirecloudGroup(eq(DUOS_ID), eq(FIRECLOUD_GROUP_NAME), anyString());
  }

  @Test
  public void testCreateFirecloudGroupWithDbInsertionFailure() {
    when(iamService.createGroup(FIRECLOUD_GROUP_NAME)).thenReturn(firecloudGroupEmail);
    when(duosDao.insertFirecloudGroup(DUOS_ID, FIRECLOUD_GROUP_NAME, firecloudGroupEmail))
        .thenReturn(false);

    assertThrows(RuntimeException.class, () -> duosService.createFirecloudGroup(DUOS_ID));
    verify(iamService, times(1)).createGroup(FIRECLOUD_GROUP_NAME);
    verify(iamService, times(1)).deleteGroup(FIRECLOUD_GROUP_NAME);
    verify(duosDao, never()).retrieveFirecloudGroup(DUOS_ID);
  }

  @Test
  public void testRetrieveOrCreateFirecloudGroup() {
    when(iamService.createGroup(FIRECLOUD_GROUP_NAME)).thenReturn(firecloudGroupEmail);
    when(duosDao.insertFirecloudGroup(DUOS_ID, FIRECLOUD_GROUP_NAME, firecloudGroupEmail))
        .thenReturn(true);
    DuosFirecloudGroupModel group = new DuosFirecloudGroupModel().duosId(DUOS_ID);
    when(duosDao.retrieveFirecloudGroup(DUOS_ID)).thenReturn(null).thenReturn(group);

    // First invocation: we create
    assertThat(duosService.retrieveOrCreateFirecloudGroup(DUOS_ID), equalTo(group));
    verify(iamService, times(1)).createGroup(FIRECLOUD_GROUP_NAME);
    verify(iamService, never()).deleteGroup(FIRECLOUD_GROUP_NAME);

    // Second invocation: we retrieve existing
    assertThat(duosService.retrieveOrCreateFirecloudGroup(DUOS_ID), equalTo(group));
    verify(iamService, times(1)).createGroup(FIRECLOUD_GROUP_NAME);
    verify(iamService, never()).deleteGroup(FIRECLOUD_GROUP_NAME);
  }
}
