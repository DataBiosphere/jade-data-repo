package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.duos.exception.DuosDatasetBadRequestException;
import bio.terra.service.duos.exception.DuosDatasetNotFoundException;
import bio.terra.service.duos.exception.DuosFirecloudGroupUpdateConflictException;
import bio.terra.service.duos.exception.DuosInternalServerErrorException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.HttpServerErrorException;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DuosServiceTest {

  @Mock private IamService iamService;
  @Mock private DuosClient duosClient;
  @Mock private DuosDao duosDao;
  private DuosService duosService;

  private static final List<String> SUBSYSTEM_NAMES =
      List.of("ontology", "elastic-search", "google-cloud-storage");
  private static final String DUOS_ID = "DUOS-123456";
  private static final String FIRECLOUD_GROUP_NAME = String.format("%s-users", DUOS_ID);
  private static final String FIRECLOUD_GROUP_EMAIL = firecloudGroupEmail(FIRECLOUD_GROUP_NAME);
  private static final DuosFirecloudGroupModel FIRECLOUD_GROUP =
      DuosFixtures.createDbFirecloudGroup(DUOS_ID);

  private static final List<String> APPROVED_USER_EMAILS =
      List.of("user1@a.com", "user2@a.com", "user3@a.com");
  private static final DuosDatasetApprovedUsers APPROVED_USERS =
      new DuosDatasetApprovedUsers(
          APPROVED_USER_EMAILS.stream().map(DuosDatasetApprovedUser::new).toList());

  @Before
  public void before() {
    duosService = new DuosService(iamService, duosClient, duosDao);
  }

  private static SystemStatusSystems statusSubsystem(boolean healthy, String name) {
    String message = name + " " + (healthy ? "" : "un") + "healthy";
    return new SystemStatusSystems(healthy, message, null, null);
  }

  @Test
  public void testStatusOk() {
    Map<String, SystemStatusSystems> subsystems =
        SUBSYSTEM_NAMES.stream()
            .collect(Collectors.toMap(Function.identity(), name -> statusSubsystem(true, name)));
    SystemStatus systemStatus = new SystemStatus(true, false, subsystems);
    when(duosClient.status()).thenReturn(systemStatus);

    RepositoryStatusModelSystems actual = duosService.status();
    assertTrue(actual.isOk());
    assertThat(actual.isCritical(), equalTo(DuosService.IS_CRITICAL_SYSTEM));
    assertThat(actual.getMessage(), equalTo(subsystems.toString()));
  }

  @Test
  public void testStatusNotOk() {
    Map<String, SystemStatusSystems> subsystems =
        SUBSYSTEM_NAMES.stream()
            .collect(Collectors.toMap(Function.identity(), name -> statusSubsystem(false, name)));
    SystemStatus systemStatus = new SystemStatus(false, false, subsystems);
    when(duosClient.status()).thenReturn(systemStatus);

    RepositoryStatusModelSystems actual = duosService.status();
    assertFalse(actual.isOk());
    assertThat(actual.isCritical(), equalTo(DuosService.IS_CRITICAL_SYSTEM));
    assertThat(actual.getMessage(), equalTo(subsystems.toString()));
  }

  @Test
  public void testStatusNotOkWhenDuosClientThrows() {
    String exceptionMessage = "Error thrown by DUOS";
    when(duosClient.status())
        .thenThrow(
            new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, exceptionMessage));

    RepositoryStatusModelSystems actual = duosService.status();
    assertFalse(actual.isOk());
    assertThat(actual.isCritical(), equalTo(DuosService.IS_CRITICAL_SYSTEM));
    assertThat(actual.getMessage(), containsString(exceptionMessage));
  }

  private static String firecloudGroupEmail(String groupName) {
    return String.format("%s@dev.test.firecloud.org", groupName);
  }

  @Test
  public void testRetrieveFirecloudGroup() {
    assertThrows(
        DuosDatasetNotFoundException.class, () -> duosService.retrieveFirecloudGroup(DUOS_ID));

    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    assertThat(duosService.retrieveFirecloudGroup(DUOS_ID), equalTo(FIRECLOUD_GROUP));
  }

  @Test
  public void testCreateFirecloudGroup() {
    when(iamService.createGroup(FIRECLOUD_GROUP_NAME)).thenReturn(FIRECLOUD_GROUP_EMAIL);

    DuosFirecloudGroupModel actual = duosService.createFirecloudGroup(DUOS_ID);
    assertThat(actual.getDuosId(), equalTo(DUOS_ID));
    assertThat(actual.getFirecloudGroupName(), equalTo(FIRECLOUD_GROUP_NAME));
    assertThat(actual.getFirecloudGroupEmail(), equalTo(FIRECLOUD_GROUP_EMAIL));
    verify(iamService).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }

  @Test
  public void testCreateFirecloudGroupWithNamingConflict() {
    String groupEmailNew = firecloudGroupEmail(FIRECLOUD_GROUP_NAME + "-new");

    // If we encounter a naming collision when trying to create a Firecloud group,
    // we don't know exactly what our backstop name will be as it will be suffixed by a new UUID,
    // but we know that it will start with our more readable group name.
    when(iamService.createGroup(startsWith(FIRECLOUD_GROUP_NAME))).thenReturn(groupEmailNew);
    doThrow(mock(IamConflictException.class)).when(iamService).createGroup(FIRECLOUD_GROUP_NAME);

    DuosFirecloudGroupModel actual = duosService.createFirecloudGroup(DUOS_ID);
    assertThat(actual.getDuosId(), equalTo(DUOS_ID));
    assertThat(actual.getFirecloudGroupName(), Matchers.startsWith(FIRECLOUD_GROUP_NAME));
    assertThat(actual.getFirecloudGroupEmail(), equalTo(groupEmailNew));
    // Our first creation attempt failed, so we tried to create again with our unique group name.
    verify(iamService, times(2)).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }

  @Test
  public void testCreateFirecloudGroupWithUnretriableCreationException() {
    doThrow(mock(IamForbiddenException.class)).when(iamService).createGroup(FIRECLOUD_GROUP_NAME);
    assertThrows(IamForbiddenException.class, () -> duosService.createFirecloudGroup(DUOS_ID));
    verify(iamService).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }

  @Test
  public void testSyncDuosDatasetAuthorizedUsers() {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID);

    verify(duosDao).retrieveFirecloudGroupByDuosId(DUOS_ID);
    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);
    verify(duosDao)
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  public void testSyncDuosDatasetAuthorizedUsersThrowsWhenNoRecord() {
    assertThrows(
        DuosDatasetNotFoundException.class,
        () -> duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID));
    verify(duosDao).retrieveFirecloudGroupByDuosId(DUOS_ID);
    verifyNoInteractions(iamService);
    verifyNoMoreInteractions(duosDao);
  }

  @Test
  public void testSyncFirecloudGroupContents() {
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    duosService.syncFirecloudGroupContents(FIRECLOUD_GROUP);

    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);
    verify(duosDao)
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  public void testSyncFirecloudGroupContentsEmptiedWhenDuosDatasetDoesNotExist() {
    when(duosClient.getApprovedUsers(DUOS_ID)).thenThrow(mock(DuosDatasetNotFoundException.class));

    duosService.syncFirecloudGroupContents(FIRECLOUD_GROUP);

    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(), IamRole.MEMBER.toString(), List.of());
    verify(duosDao)
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  public void testSyncFirecloudGroupContentsThrowsWhenUserFetchThrows() {
    DuosInternalServerErrorException expectedEx = mock(DuosInternalServerErrorException.class);
    when(duosClient.getApprovedUsers(DUOS_ID)).thenThrow(expectedEx);

    DuosInternalServerErrorException actualEx =
        assertThrows(
            DuosInternalServerErrorException.class,
            () -> duosService.syncFirecloudGroupContents(FIRECLOUD_GROUP));
    assertThat(actualEx, equalTo(expectedEx));

    verify(iamService, never())
        .overwriteGroupPolicyEmails(
            eq(FIRECLOUD_GROUP.getFirecloudGroupName()), eq(IamRole.MEMBER.toString()), any());
    verify(duosDao, never())
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  public void testSyncFirecloudGroupContentsThrowsWhenIamServiceThrows() {
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    IamForbiddenException expectedEx = mock(IamForbiddenException.class);
    doThrow(expectedEx)
        .when(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);

    IamForbiddenException actualEx =
        assertThrows(
            IamForbiddenException.class,
            () -> duosService.syncFirecloudGroupContents(FIRECLOUD_GROUP));
    assertThat(actualEx, equalTo(expectedEx));
    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);
    verify(duosDao, never())
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  public void testSyncFirecloudGroupContentsThrowsWhenDbUpdateThrows() {
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    when(duosDao.updateFirecloudGroupLastSyncedDate(
            eq(FIRECLOUD_GROUP.getId()), isA(Instant.class)))
        .thenThrow(mock(RuntimeException.class));
    assertThrows(
        RuntimeException.class, () -> duosService.syncFirecloudGroupContents(FIRECLOUD_GROUP));

    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);
    verify(duosDao)
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  public void testUpdateLastSyncedThrowsCustomExceptionOnConflict() {
    CannotSerializeTransactionException expectedEx =
        mock(CannotSerializeTransactionException.class);
    when(duosDao.updateFirecloudGroupLastSyncedDate(
            eq(FIRECLOUD_GROUP.getId()), isA(Instant.class)))
        .thenThrow(expectedEx);

    DuosFirecloudGroupUpdateConflictException actualEx =
        assertThrows(
            DuosFirecloudGroupUpdateConflictException.class,
            () -> duosService.updateLastSynced(FIRECLOUD_GROUP, Instant.now()));
    assertThat(actualEx.getCause(), equalTo(expectedEx));
  }

  @Test
  public void testUpdateLastSyncedThrowsOtherDataAccessExceptions() {
    DataAccessException expectedEx = mock(DataAccessException.class);
    when(duosDao.updateFirecloudGroupLastSyncedDate(
            eq(FIRECLOUD_GROUP.getId()), isA(Instant.class)))
        .thenThrow(expectedEx);

    DataAccessException actualEx =
        assertThrows(
            DataAccessException.class,
            () -> duosService.updateLastSynced(FIRECLOUD_GROUP, Instant.now()));
    assertThat(actualEx, equalTo(expectedEx));
  }

  @Test
  public void testGetAuthorizedUsers() {
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);
    assertThat(duosService.getAuthorizedUsers(DUOS_ID), equalTo(APPROVED_USER_EMAILS));
  }

  @Test
  public void testGetAuthorizedUsersReturnsEmptyListWhenDatasetBadRequest() {
    when(duosClient.getApprovedUsers(DUOS_ID))
        .thenThrow(mock(DuosDatasetBadRequestException.class));
    assertThat(duosService.getAuthorizedUsers(DUOS_ID), empty());
  }

  @Test
  public void testGetAuthorizedUsersReturnsEmptyListWhenDatasetNotFound() {
    when(duosClient.getApprovedUsers(DUOS_ID)).thenThrow(mock(DuosDatasetNotFoundException.class));
    assertThat(duosService.getAuthorizedUsers(DUOS_ID), empty());
  }

  @Test
  public void testGetAuthorizedUsersThrowsWhenUnexpectedError() {
    DuosInternalServerErrorException expectedEx = mock(DuosInternalServerErrorException.class);
    when(duosClient.getApprovedUsers(DUOS_ID)).thenThrow(expectedEx);
    DuosInternalServerErrorException actualEx =
        assertThrows(
            DuosInternalServerErrorException.class, () -> duosService.getAuthorizedUsers(DUOS_ID));
    assertThat(actualEx, equalTo(expectedEx));
  }
}
