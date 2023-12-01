package bio.terra.service.dataset.flight.lock;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetLockFlight extends Flight {
  public DatasetLockFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    JournalService journalService = appContext.getBean(JournalService.class);

    // Input parameters
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    // Configurations
    RetryRule lockDatasetRetry =
        getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads());

    // Steps
    addStep(new LockDatasetStep(datasetService, datasetId, false, false), lockDatasetRetry);
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService, userReq, datasetId, IamResourceType.DATASET, "Dataset locked", true));
  }
}
