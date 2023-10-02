package bio.terra.service.dataset.flight.upgrade.disableSecureMonitoring;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring.SecureMonitoringEnableFlagStep;
import bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring.SecureMonitoringRecordInFlightMapStep;
import bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring.SecureMonitoringRefolderGcpProjectsStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DisableSecureMonitoringFlight extends Flight {

  public DisableSecureMonitoringFlight(FlightMap inputParameters, Object applicationContext) {
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
      addStep(new SecureMonitoringEnableFlagStep(datasetDao, userReq, false));
      addStep(
          new SecureMonitoringRefolderGcpProjectsStep(
              dataset, snapshotService, bufferService, userReq, false));
      addStep(
          new DisableSecureMonitoringDeleteSourceDatasetAndSnapshotsTpsPolicyStep(
              snapshotService, policyService, userReq));
      addStep(new DisableSecureMonitoringJournalEntryStep(journalService, userReq));
      addStep(new UnlockDatasetStep(datasetService, datasetId, false));
    } else {
      throw new FeatureNotImplementedException(
          "Updating an exisiting dataset to use secure monitoring is only supported on GCP");
    }
  }
}
