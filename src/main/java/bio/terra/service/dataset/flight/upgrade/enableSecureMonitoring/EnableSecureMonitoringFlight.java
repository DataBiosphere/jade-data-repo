package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class EnableSecureMonitoringFlight extends Flight {

  public EnableSecureMonitoringFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;

    DatasetService datasetService = appContext.getBean(DatasetService.class);
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    BufferService bufferService = appContext.getBean(BufferService.class);
    PolicyService policyService = appContext.getBean(PolicyService.class);
    JournalService journalService = appContext.getBean(JournalService.class);
    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    UUID datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), UUID.class);

    Dataset dataset = datasetService.retrieve(datasetId);
    CloudPlatformWrapper platform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    if (platform.isGcp()) {
      addStep(new LockDatasetStep(datasetService, datasetId, false));
      addStep(new SecureMonitoringRecordInFlightMapStep(dataset));
      addStep(new SecureMonitoringSetFlagStep(datasetDao, userReq, true));
      addStep(
          new SecureMonitoringRefolderGcpProjectsStep(
              dataset, snapshotService, bufferService, userReq, true));
      addStep(
          new EnableSecureMonitoringCreateSnapshotsTpsPolicyStep(
              snapshotService, policyService, userReq));
      addStep(new EnableSecureMonitoringJournalEntryStep(journalService, userReq));
      addStep(new UnlockDatasetStep(datasetService, datasetId, false));
    } else {
      throw new FeatureNotImplementedException(
          "Updating an existing dataset to use secure monitoring is only supported on GCP");
    }
  }
}
