package bio.terra.service.load.flight;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadLockKey;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;

public class LoadUnlockStep extends DefaultUndoStep {
  private final LoadService loadService;
  private final String userSuppliedLockName;

  /**
   * This step is meant to be shared by dataset and filesystem flights for removing {@code
   * lockName}'s lock held on the load tag in the targeted dataset.
   *
   * <p>It expects the following to be available in the flight context:
   *
   * <ul>
   *   <li>(Optional) {@link LoadMapKeys#LOAD_TAG} in the input parameters or working map (if
   *       unavailable, any lock associated with the dataset and {@code lockName} will be cleared)
   *   <li>{@link JobMapKeys#DATASET_ID} in the input parameters
   * </ul>
   *
   * @param lockName the name of the lock to remove (if unspecified, defaults to this flight's ID)
   */
  public LoadUnlockStep(LoadService loadService, String lockName) {
    this.loadService = loadService;
    this.userSuppliedLockName = lockName;
  }

  /**
   * This step is meant to be shared by dataset and filesystem flights for removing this flight's
   * lock held on the load tag in the targeted dataset.
   *
   * <p>It expects the following to be available in the flight context:
   *
   * <ul>
   *   <li>(Optional) {@link LoadMapKeys#LOAD_TAG} in the input parameters or working map (if
   *       unavailable, any lock associated with the dataset and this flight's ID will be cleared)
   *   <li>{@link JobMapKeys#DATASET_ID} in the input parameters
   * </ul>
   */
  public LoadUnlockStep(LoadService loadService) {
    this(loadService, null);
  }

  @VisibleForTesting
  public String getUserSuppliedLockName() {
    return this.userSuppliedLockName;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    String lockName = getLockName(context);
    try {
      LoadLockKey loadLockKey = loadService.getLoadLockKey(context);
      loadService.unlockLoad(loadLockKey, lockName);
    } catch (LoadLockFailureException ex) {
      // The flight context may not have a load tag.  We'll clear any lock associated with the
      // flight's dataset and lock name in that case.
      loadService.unlockLoad(IngestUtils.getDatasetId(context), lockName);
    }
    return StepResult.getStepResultSuccess();
  }

  /**
   * @return the name of the lock to remove (if unspecified, defaults to this flight's ID)
   */
  private String getLockName(FlightContext context) {
    return Optional.ofNullable(userSuppliedLockName).orElse(context.getFlightId());
  }
}
