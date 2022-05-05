package bio.terra.service.dataset.flight.update;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetSchemaUpdateFlight extends Flight {

  public DatasetSchemaUpdateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetTableDao datasetTableDao = appContext.getBean(DatasetTableDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);

    DatasetSchemaUpdateModel updateModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetSchemaUpdateModel.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    addStep(new DatasetSchemaUpdateValidateModelStep(datasetService, datasetId, updateModel));
    addStep(new LockDatasetStep(datasetService, datasetId, false));

    if (DatasetSchemaUpdateUtils.hasTableAdditions(updateModel)) {
      addStep(new DatasetSchemaUpdateAddTablesStep(datasetTableDao, datasetId, updateModel));
    }

    addStep(new UnlockDatasetStep(datasetService, datasetId, false));
  }
}
