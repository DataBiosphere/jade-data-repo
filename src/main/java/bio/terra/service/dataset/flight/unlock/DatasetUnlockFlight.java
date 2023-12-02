package bio.terra.service.dataset.flight.unlock;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.UnlockResourceRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.common.UnlockResourceCheckJobStateStep;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetUnlockFlight extends Flight {
  public DatasetUnlockFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    JournalService journalService = appContext.getBean(JournalService.class);
    JobService jobService = appContext.getBean(JobService.class);

    // Input parameters
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    var unlockRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UnlockResourceRequest.class);
    String lockName = unlockRequest.getLockName();

    // Configurations
    RetryRule unlockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    // Steps
    addStep(new UnlockDatasetCheckLockNameStep(datasetService, datasetId, lockName));
    if (unlockRequest.isForceUnlock()) {
      addStep(new UnlockResourceCheckJobStateStep(jobService, lockName));
    }
    addStep(
        new UnlockDatasetStep(datasetService, datasetId, false, lockName, true),
        unlockDatasetRetry);
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService, userReq, datasetId, IamResourceType.DATASET, "Dataset unlocked."));
  }
}
