package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.CommonExceptions;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.dataset.flight.upgrade.predictableFileIds.*;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class EnableSecureMonitoringFlight extends Flight {

  public EnableSecureMonitoringFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;

    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    //    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    //    BufferService bufferService = appContext.getBean(BufferService.class);

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    UUID datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), UUID.class);

    // confirm dataset is hosted on GCP
    Dataset dataset = datasetService.retrieve(datasetId);
    CloudPlatformWrapper platform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    if (platform.isGcp()) {
      addStep(new LockDatasetStep(datasetService, datasetId, false));

      // flip flag in dataset table that secure monitoring is enabled
      addStep(new EnableSecureMonitoringFlipFlagStep(datasetId, datasetDao, userReq));

      // Refolder dataset and child snapshot GCP projects

      // Register snapshots in TPS

      addStep(new UnlockDatasetStep(datasetService, datasetId, false));
    } else if (platform.isAzure()) {
      throw CommonExceptions.TRANSACTIONS_NOT_IMPLEMENTED_IN_AZURE;
    }
  }
}
