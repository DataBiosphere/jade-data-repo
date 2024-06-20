package bio.terra.service.duos;

import bio.terra.common.ExceptionUtils;
import bio.terra.common.FutureUtils;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.DuosFirecloudGroupsSyncResponse;
import bio.terra.model.ErrorModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.auth.iam.FirecloudGroupModel;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.duos.exception.DuosDatasetBadRequestException;
import bio.terra.service.duos.exception.DuosDatasetNotFoundException;
import bio.terra.service.duos.exception.DuosFirecloudGroupUpdateConflictException;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DuosService {
  private static final Logger logger = LoggerFactory.getLogger(DuosService.class);
  static final boolean IS_CRITICAL_SYSTEM = false;
  private final IamService iamService;
  private final DuosClient duosClient;
  private final DuosDao duosDao;
  private final ExecutorService executor;

  public DuosService(
      IamService iamService,
      DuosClient duosClient,
      DuosDao duosDao,
      @Qualifier("performanceThreadpool") ExecutorService executor) {
    this.iamService = iamService;
    this.duosClient = duosClient;
    this.duosDao = duosDao;
    this.executor = executor;
  }

  /**
   * @return status of DUOS and its subsystems
   */
  public RepositoryStatusModelSystems status() {
    try {
      SystemStatus status = duosClient.status();
      return new RepositoryStatusModelSystems()
          .ok(status.ok())
          .critical(IS_CRITICAL_SYSTEM)
          .message(status.systems().toString());
    } catch (Exception ex) {
      String errorMsg = "DUOS status check failed";
      logger.error(errorMsg, ex);
      return new RepositoryStatusModelSystems()
          .ok(false)
          .critical(IS_CRITICAL_SYSTEM)
          .message(errorMsg + ": " + ExceptionUtils.formatException(ex));
    }
  }

  /**
   * @return all TDR-managed DUOS Firecloud groups
   */
  public List<DuosFirecloudGroupModel> retrieveFirecloudGroups() {
    return duosDao.retrieveFirecloudGroups();
  }

  /**
   * @param duosId DUOS dataset ID
   * @return TDR-managed DUOS Firecloud group
   * @throws DuosDatasetNotFoundException if TDR has no record of the DUOS dataset
   */
  public DuosFirecloudGroupModel retrieveFirecloudGroup(String duosId) {
    DuosFirecloudGroupModel firecloudGroup = duosDao.retrieveFirecloudGroupByDuosId(duosId);
    if (firecloudGroup == null) {
      throw new DuosDatasetNotFoundException(
          "DUOS dataset %s has not been registered in TDR".formatted(duosId));
    }
    return firecloudGroup;
  }

  /**
   * @param duosId DUOS dataset ID
   * @return a representation of the newly created Firecloud managed group. Note that it will be
   *     partially populated, as several metadata elements are intended to be generated by DuosDao
   *     and the DB on insert.
   */
  public DuosFirecloudGroupModel createFirecloudGroup(String duosId) {
    logger.info("Creating Firecloud group for {} users", duosId);

    FirecloudGroupModel firecloudGroup = iamService.createFirecloudGroup(duosId);
    String groupName = firecloudGroup.getGroupName();
    logger.info("Successfully created Firecloud group {} for {} users", groupName, duosId);
    return new DuosFirecloudGroupModel()
        .duosId(duosId)
        .firecloudGroupName(groupName)
        .firecloudGroupEmail(firecloudGroup.getGroupEmail());
  }

  /**
   * If the DUOS dataset ID is registered in TDR, force a sync of its Firecloud group members.
   *
   * @param duosId DUOS dataset ID
   * @return DUOS Firecloud group reflecting the last successful sync of its members
   */
  public DuosFirecloudGroupModel syncDuosDatasetAuthorizedUsers(String duosId) {
    DuosFirecloudGroupModel firecloudGroup = retrieveFirecloudGroup(duosId);
    Instant lastSyncedDate = Instant.now();
    iamService.overwriteGroupPolicyEmails(
        firecloudGroup.getFirecloudGroupName(),
        IamRole.MEMBER.toString(),
        getAuthorizedUsers(firecloudGroup.getDuosId()));
    updateLastSynced(firecloudGroup, lastSyncedDate);
    return duosDao.retrieveFirecloudGroup(firecloudGroup.getId());
  }

  /**
   * @param firecloudGroup DUOS Firecloud group whose members were overwritten by the latest set of
   *     approved users
   * @param lastSyncedDate DUOS Firecloud group members match the DUOS dataset's authorized users at
   *     this moment in time
   */
  private void updateLastSynced(DuosFirecloudGroupModel firecloudGroup, Instant lastSyncedDate) {
    UUID id = firecloudGroup.getId();
    try {
      duosDao.updateFirecloudGroupLastSyncedDate(id, lastSyncedDate);
    } catch (PessimisticLockingFailureException ex) {
      String message =
          firecloudGroup.getFirecloudGroupEmail()
              + " members were updated, "
              + "but an error occurred when updating its database record with last_synced_date "
              + lastSyncedDate;
      String errorDetail =
          "This indicates that another process was updating the database row at the same time";
      throw new DuosFirecloudGroupUpdateConflictException(message, ex, List.of(errorDetail));
    }
  }

  /**
   * @param id of the DUOS Firecloud Group record if it was successfully synced, null otherwise
   * @param error encountered if the DUOS Firecloud Group failed to sync, null otherwise
   */
  record SyncResult(UUID id, ErrorModel error) {}

  /**
   * For all DUOS dataset IDs registered in TDR, force a sync of their Firecloud group members. A
   * failure to sync any Firecloud group(s) should not block attempts to sync the rest.
   *
   * @return DUOS Firecloud groups whose members were successfully synced, and any errors occurred
   *     which may have interfered with syncing
   */
  @Scheduled(cron = "${duos.syncUsers.schedule:-}")
  @SchedulerLock(
      name = "DuosService.syncDuosDatasetsAuthorizedUsers",
      lockAtLeastFor = "5s",
      lockAtMostFor = "5m")
  public DuosFirecloudGroupsSyncResponse syncDuosDatasetsAuthorizedUsers() {
    // 1. Parallelize our calls to external systems (DUOS, Sam) which perform the sync
    Instant lastSyncedDate = Instant.now();
    List<Future<SyncResult>> futures =
        duosDao.retrieveFirecloudGroups().stream()
            .map(group -> executor.submit(() -> syncFirecloudGroupContents(group)))
            .toList();
    List<SyncResult> results = FutureUtils.waitFor(futures);

    // 2. Record successful syncs to the DB in a single call to avoid deadlocks
    List<UUID> syncedIds = results.stream().map(SyncResult::id).filter(Objects::nonNull).toList();
    Optional<ErrorModel> maybeDbError = batchUpdateLastSynced(syncedIds, lastSyncedDate);

    // 3. Collect any errors emitted along the way.
    List<ErrorModel> errors =
        results.stream()
            .map(SyncResult::error)
            .filter(Objects::nonNull)
            .collect(Collectors.toCollection(ArrayList::new)); // We need a modifiable list here
    maybeDbError.ifPresent(errors::add);

    return new DuosFirecloudGroupsSyncResponse()
        .synced(duosDao.retrieveFirecloudGroups(syncedIds))
        .errors(errors);
  }

  @VisibleForTesting
  static String syncFirecloudGroupContentsErrorMessage(DuosFirecloudGroupModel firecloudGroup) {
    return "Error syncing contents of %s".formatted(firecloudGroup.getFirecloudGroupEmail());
  }

  /**
   * @param firecloudGroup DUOS Firecloud group whose members should be overwritten by the latest
   *     set of approved users
   * @return the id of the group if it synced successfully, or the error encountered if not
   */
  private SyncResult syncFirecloudGroupContents(DuosFirecloudGroupModel firecloudGroup) {
    try {
      iamService.overwriteGroupPolicyEmails(
          firecloudGroup.getFirecloudGroupName(),
          IamRole.MEMBER.toString(),
          getAuthorizedUsers(firecloudGroup.getDuosId()));
      return new SyncResult(firecloudGroup.getId(), null);
    } catch (Exception ex) {
      String message = syncFirecloudGroupContentsErrorMessage(firecloudGroup);
      logger.error(message, ex);
      ErrorModel error = new ErrorModel().message(message).addErrorDetailItem(ex.getMessage());
      return new SyncResult(null, error);
    }
  }

  @VisibleForTesting
  static String updatedTooFewRecordsMessage(int numSyncedIds, int numRecordsUpdated) {
    return "Expected to update %d records but only updated %d"
        .formatted(numSyncedIds, numRecordsUpdated);
  }

  /**
   * Record successful Firecloud group syncs to the DB in single call to avoid deadlocks.
   *
   * @param syncedIds record IDs for those Firecloud groups successfully synced
   * @param lastSyncedDate DUOS Firecloud group members match the DUOS dataset's authorized users at
   *     this moment in time
   * @return error if encountered while writing to the database
   */
  private Optional<ErrorModel> batchUpdateLastSynced(List<UUID> syncedIds, Instant lastSyncedDate) {
    try {
      int numRecordsUpdated =
          duosDao.updateFirecloudGroupsLastSyncedDate(syncedIds, lastSyncedDate);
      if (numRecordsUpdated < syncedIds.size()) {
        String message = updatedTooFewRecordsMessage(syncedIds.size(), numRecordsUpdated);
        return Optional.of(new ErrorModel().message(message));
      }
    } catch (Exception ex) {
      String message =
          "Some Firecloud groups were updated, "
              + "but an error occurred updating records with last_synced_time "
              + lastSyncedDate;
      logger.error(message, ex);
      return Optional.of(new ErrorModel().message(message).addErrorDetailItem(ex.getMessage()));
    }
    return Optional.empty();
  }

  /**
   * @param duosId DUOS dataset ID
   * @return a list of emails belonging to the DUOS dataset's approved users, empty if the dataset
   *     is known to not exist. Unexpected exceptions are allowed to bubble up to register as a
   *     failure to sync the Firecloud group's contents.
   */
  private List<String> getAuthorizedUsers(String duosId) {
    List<String> users;
    try {
      users =
          duosClient.getApprovedUsers(duosId).approvedUsers().stream()
              .map(DuosDatasetApprovedUser::email)
              .toList();
    } catch (DuosDatasetBadRequestException | DuosDatasetNotFoundException ex) {
      logger.warn("No DUOS dataset exists for ID {}, emptying its Firecloud group members", duosId);
      users = List.of();
    }
    return users;
  }
}
