package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.ErrorModel;
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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DuosServiceTest {

  @Mock private IamService iamService;
  @Mock private DuosClient duosClient;
  @Mock private DuosDao duosDao;

  private ExecutorService executor;
  private DuosService duosService;

  private static final int NUM_EXECUTOR_THREADS = 3;

  private static final List<String> SUBSYSTEM_NAMES =
      List.of("duos_subsystem_1", "duos_subsystem_2", "duos_subsystem_3");

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

  @BeforeEach
  void beforeEach() {
    executor = Executors.newFixedThreadPool(NUM_EXECUTOR_THREADS);
    duosService = new DuosService(iamService, duosClient, duosDao, executor);
  }

  @AfterEach
  void afterEach() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private static SystemStatusSystems statusSubsystem(boolean healthy, String name) {
    String message = name + " " + (healthy ? "" : "un") + "healthy";
    return new SystemStatusSystems(healthy, message, null, null);
  }

  @Test
  void testStatusOk() {
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
  void testStatusNotOk() {
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
  void testStatusNotOkWhenDuosClientThrows() {
    String exceptionMessage = "Error thrown by DUOS";
    when(duosClient.status())
        .thenThrow(
            new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, exceptionMessage));

    RepositoryStatusModelSystems actual = duosService.status();
    assertFalse(actual.isOk());
    assertThat(actual.isCritical(), equalTo(DuosService.IS_CRITICAL_SYSTEM));
    assertThat(actual.getMessage(), containsString(exceptionMessage));
  }

  private static String firecloudGroupName(String duosId) {
    return String.format("%s-users", duosId);
  }

  private static String firecloudGroupEmail(String duosId) {
    return String.format("%s@dev.test.firecloud.org", firecloudGroupName(duosId));
  }

  @Test
  void testRetrieveFirecloudGroup() {
    assertThrows(
        DuosDatasetNotFoundException.class, () -> duosService.retrieveFirecloudGroup(DUOS_ID));

    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    assertThat(duosService.retrieveFirecloudGroup(DUOS_ID), equalTo(FIRECLOUD_GROUP));
  }

  @Test
  void testCreateFirecloudGroup() {
    when(iamService.createGroup(FIRECLOUD_GROUP_NAME)).thenReturn(FIRECLOUD_GROUP_EMAIL);

    DuosFirecloudGroupModel actual = duosService.createFirecloudGroup(DUOS_ID);
    assertThat(actual.getDuosId(), equalTo(DUOS_ID));
    assertThat(actual.getFirecloudGroupName(), equalTo(FIRECLOUD_GROUP_NAME));
    assertThat(actual.getFirecloudGroupEmail(), equalTo(FIRECLOUD_GROUP_EMAIL));
    verify(iamService).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }

  @Test
  void testCreateFirecloudGroupWithNamingConflict() {
    String groupEmailNew = firecloudGroupEmail(FIRECLOUD_GROUP_NAME + "-new");

    // If we encounter a naming collision when trying to create a Firecloud group,
    // we don't know exactly what our backstop name will be as it will be suffixed by a new UUID,
    // but we know that it will start with our more readable group name.
    when(iamService.createGroup(startsWith(FIRECLOUD_GROUP_NAME))).thenReturn(groupEmailNew);
    doThrow(new IamConflictException("", List.of()))
        .when(iamService)
        .createGroup(FIRECLOUD_GROUP_NAME);

    DuosFirecloudGroupModel actual = duosService.createFirecloudGroup(DUOS_ID);
    assertThat(actual.getDuosId(), equalTo(DUOS_ID));
    assertThat(actual.getFirecloudGroupName(), Matchers.startsWith(FIRECLOUD_GROUP_NAME));
    assertThat(actual.getFirecloudGroupEmail(), equalTo(groupEmailNew));
    // Our first creation attempt failed, so we tried to create again with our unique group name.
    verify(iamService, times(2)).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }

  @Test
  void testCreateFirecloudGroupWithUnretriableCreationException() {
    doThrow(new IamForbiddenException("", List.of()))
        .when(iamService)
        .createGroup(FIRECLOUD_GROUP_NAME);
    assertThrows(IamForbiddenException.class, () -> duosService.createFirecloudGroup(DUOS_ID));
    verify(iamService).createGroup(startsWith(FIRECLOUD_GROUP_NAME));
  }

  @Test
  void testSyncDuosDatasetAuthorizedUsers() {
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
  void testSyncDuosDatasetAuthorizedUsersThrowsWhenNoRecord() {
    assertThrows(
        DuosDatasetNotFoundException.class,
        () -> duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID));
    verify(duosDao).retrieveFirecloudGroupByDuosId(DUOS_ID);
    verifyNoInteractions(iamService);
    verifyNoMoreInteractions(duosDao);
  }

  @Test
  void testSyncDuosDatasetAuthorizedUsersEmptiesGroupWhenDuosDatasetDoesNotExist() {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    when(duosClient.getApprovedUsers(DUOS_ID)).thenThrow(new DuosDatasetNotFoundException(""));

    duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID);

    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(), IamRole.MEMBER.toString(), List.of());
    verify(duosDao)
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  void testSyncDuosDatasetAuthorizedUsersEmptiesGroupWhenBadDuosDatasetRequest() {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    when(duosClient.getApprovedUsers(DUOS_ID)).thenThrow(new DuosDatasetBadRequestException(""));

    duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID);

    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(), IamRole.MEMBER.toString(), List.of());
    verify(duosDao)
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  void testSyncDuosDatasetAuthorizedUsersThrowsWhenUserFetchThrows() {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    var expectedEx = new DuosInternalServerErrorException("Could not get approved users");
    when(duosClient.getApprovedUsers(DUOS_ID)).thenThrow(expectedEx);

    DuosInternalServerErrorException actualEx =
        assertThrows(
            DuosInternalServerErrorException.class,
            () -> duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID));
    assertThat(actualEx, equalTo(expectedEx));

    verify(iamService, never())
        .overwriteGroupPolicyEmails(
            eq(FIRECLOUD_GROUP.getFirecloudGroupName()), eq(IamRole.MEMBER.toString()), any());
    verify(duosDao, never())
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  void testSyncDuosDatasetAuthorizedUsersThrowsWhenIamServiceThrows() {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    IamForbiddenException expectedEx = new IamForbiddenException("Forbidden", List.of());
    doThrow(expectedEx)
        .when(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);

    IamForbiddenException actualEx =
        assertThrows(
            IamForbiddenException.class, () -> duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID));
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
  void testSyncDuosDatasetAuthorizedUsersThrowsCustomExceptionOnDbUpdateConflict() {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    var expectedEx = new PessimisticLockingFailureException("Conflict on update");
    when(duosDao.updateFirecloudGroupLastSyncedDate(
            eq(FIRECLOUD_GROUP.getId()), isA(Instant.class)))
        .thenThrow(expectedEx);
    DuosFirecloudGroupUpdateConflictException actualEx =
        assertThrows(
            DuosFirecloudGroupUpdateConflictException.class,
            () -> duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID));
    assertThat(actualEx.getCause(), equalTo(expectedEx));

    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);
    verify(duosDao)
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  void testSyncDuosDatasetAuthorizedUsersThrowsOnOtherDbExceptions() {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(FIRECLOUD_GROUP);
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    var expectedEx = new RuntimeException();
    when(duosDao.updateFirecloudGroupLastSyncedDate(
            eq(FIRECLOUD_GROUP.getId()), isA(Instant.class)))
        .thenThrow(expectedEx);
    RuntimeException actualEx =
        assertThrows(
            RuntimeException.class, () -> duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID));
    assertThat(actualEx, equalTo(expectedEx));

    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);
    verify(duosDao)
        .updateFirecloudGroupLastSyncedDate(eq(FIRECLOUD_GROUP.getId()), isA(Instant.class));
  }

  @Test
  void testSyncDuosDatasetsAuthorizedUsersWithSyncErrors() {
    String duosIdDuosCallThrows = "DUOS-DUOSTHROWS";
    String duosIdSamCallThrows = "DUOS-SAMTHROWS";

    Map<String, DuosFirecloudGroupModel> groupMap =
        Map.of(
            DUOS_ID, FIRECLOUD_GROUP,
            duosIdDuosCallThrows, DuosFixtures.createDbFirecloudGroup(duosIdDuosCallThrows),
            duosIdSamCallThrows, DuosFixtures.createDbFirecloudGroup(duosIdSamCallThrows));
    when(duosDao.retrieveFirecloudGroups()).thenReturn(groupMap.values().stream().toList());

    // Will successfully sync and be written back to the DB
    UUID syncedId = FIRECLOUD_GROUP.getId();
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);
    when(duosDao.updateFirecloudGroupsLastSyncedDate(eq(List.of(syncedId)), any(Instant.class)))
        .thenReturn(1);
    when(duosDao.retrieveFirecloudGroups(List.of(syncedId))).thenReturn(List.of(FIRECLOUD_GROUP));

    // Will fail sync due to DUOS exception
    var expectedDuosEx = new DuosInternalServerErrorException("Could not get approved users");
    when(duosClient.getApprovedUsers(duosIdDuosCallThrows)).thenThrow(expectedDuosEx);

    // Will fail sync due to IAM exception
    when(duosClient.getApprovedUsers(duosIdSamCallThrows)).thenReturn(APPROVED_USERS);
    IamForbiddenException expectedIamEx = new IamForbiddenException("Forbidden", List.of());
    // Strict stubbing requires us to define this default behavior before stubbing with an exception
    doNothing().when(iamService).overwriteGroupPolicyEmails(any(), any(), any());
    doThrow(expectedIamEx)
        .when(iamService)
        .overwriteGroupPolicyEmails(
            groupMap.get(duosIdSamCallThrows).getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);

    var response = duosService.syncDuosDatasetsAuthorizedUsers();

    // We attempt to fetch approved users for all of TDR's Firecloud groups
    verify(duosClient, times(3)).getApprovedUsers(any());
    // We attempt to sync users only for the Firecloud groups whose DUOS fetches succeeded
    verify(iamService, times(2))
        .overwriteGroupPolicyEmails(any(), eq(IamRole.MEMBER.toString()), any());
    // We attempt to update DB records only for the Firecloud group whose IAM sync succeeded
    verify(duosDao).updateFirecloudGroupsLastSyncedDate(eq(List.of(syncedId)), any(Instant.class));

    assertThat(
        "Only the successfully synced Firecloud group is returned",
        response.getSynced(),
        contains(FIRECLOUD_GROUP));

    var errors = response.getErrors();
    assertThat("2 errors encountered during batch sync", errors, hasSize(2));
    var errorMessages = errors.stream().map(ErrorModel::getMessage).toList();
    var expectedErrorMessages =
        Stream.of(duosIdDuosCallThrows, duosIdSamCallThrows)
            .map(duosId -> DuosService.syncFirecloudGroupContentsErrorMessage(groupMap.get(duosId)))
            .toArray();
    assertThat(
        "The attempted syncs which threw exceptions are included in the response errors",
        errorMessages,
        containsInAnyOrder(expectedErrorMessages));
  }

  @Test
  void testSyncDuosDatasetsAuthorizedUsersWithTooFewRecordsUpdated() {
    UUID id = FIRECLOUD_GROUP.getId();
    when(duosDao.retrieveFirecloudGroups()).thenReturn(List.of(FIRECLOUD_GROUP));
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    int numRecordsUpdated = 0;
    when(duosDao.updateFirecloudGroupsLastSyncedDate(eq(List.of(id)), any(Instant.class)))
        .thenReturn(numRecordsUpdated);
    when(duosDao.retrieveFirecloudGroups(List.of(id))).thenReturn(List.of(FIRECLOUD_GROUP));

    var response = duosService.syncDuosDatasetsAuthorizedUsers();

    verify(duosClient).getApprovedUsers(DUOS_ID);
    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);
    verify(duosDao).updateFirecloudGroupsLastSyncedDate(eq(List.of(id)), any(Instant.class));

    assertThat(
        "The successfully synced Firecloud group is returned",
        response.getSynced(),
        contains(FIRECLOUD_GROUP));

    var errors = response.getErrors();
    assertThat("1 error encountered during batch sync", errors, hasSize(1));
    assertThat(
        "Failure to update the expected number of records is reflected in errors",
        errors.get(0).getMessage(),
        equalTo(DuosService.updatedTooFewRecordsMessage(1, numRecordsUpdated)));
  }

  @Test
  void testSyncDuosDatasetsAuthorizedUsersWithDbError() {
    UUID id = FIRECLOUD_GROUP.getId();
    when(duosDao.retrieveFirecloudGroups()).thenReturn(List.of(FIRECLOUD_GROUP));
    when(duosClient.getApprovedUsers(DUOS_ID)).thenReturn(APPROVED_USERS);

    var expectedException = new RuntimeException("DB error when recording last_synced_date");
    when(duosDao.updateFirecloudGroupsLastSyncedDate(eq(List.of(id)), any(Instant.class)))
        .thenThrow(expectedException);
    when(duosDao.retrieveFirecloudGroups(List.of(id))).thenReturn(List.of(FIRECLOUD_GROUP));

    var response = duosService.syncDuosDatasetsAuthorizedUsers();

    verify(duosClient).getApprovedUsers(DUOS_ID);
    verify(iamService)
        .overwriteGroupPolicyEmails(
            FIRECLOUD_GROUP.getFirecloudGroupName(),
            IamRole.MEMBER.toString(),
            APPROVED_USER_EMAILS);
    verify(duosDao).updateFirecloudGroupsLastSyncedDate(eq(List.of(id)), any(Instant.class));

    assertThat(
        "The successfully synced Firecloud group is returned",
        response.getSynced(),
        contains(FIRECLOUD_GROUP));

    var errors = response.getErrors();
    assertThat("1 error encountered during batch sync", errors, hasSize(1));
    var errorDetails = errors.get(0).getErrorDetail();
    assertThat(errorDetails, hasSize(1));
    assertThat(
        "DB error is reflected in errors",
        errorDetails.get(0),
        equalTo(expectedException.getMessage()));
  }
}
