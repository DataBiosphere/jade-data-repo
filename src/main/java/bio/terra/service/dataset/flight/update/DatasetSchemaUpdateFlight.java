package bio.terra.service.dataset.flight.update;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetRelationshipDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class DatasetSchemaUpdateFlight extends Flight {

  public DatasetSchemaUpdateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    DatasetTableDao datasetTableDao = appContext.getBean(DatasetTableDao.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    DatasetRelationshipDao relationshipDao = appContext.getBean(DatasetRelationshipDao.class);
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);

    AuthenticatedUserRequest userRequest =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    DatasetSchemaUpdateModel updateModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetSchemaUpdateModel.class);

    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));

    Dataset dataset = datasetDao.retrieve(datasetId);
    CloudPlatformWrapper cloudPlatform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getCloudPlatform());

    if (cloudPlatform.isAzure()) {
      throw new InvalidCloudPlatformException("Cannot update table schema for Azure datasets");
    }

    addStep(new LockDatasetStep(datasetService, datasetId, false));

    addStep(new DatasetSchemaUpdateValidateModelStep(datasetService, datasetId, updateModel));

    if (DatasetSchemaUpdateUtils.hasTableAdditions(updateModel)) {
      addStep(
          new DatasetSchemaUpdateAddTablesPostgresStep(datasetTableDao, datasetId, updateModel));
      addStep(
          new DatasetSchemaUpdateAddTablesBigQueryStep(
              bigQueryDatasetPdao, datasetDao, datasetId, updateModel));
    }

    if (DatasetSchemaUpdateUtils.hasColumnAdditions(updateModel)) {
      addStep(
          new DatasetSchemaUpdateAddColumnsPostgresStep(datasetTableDao, datasetId, updateModel));
      addStep(
          new DatasetSchemaUpdateAddColumnsBigQueryStep(
              bigQueryDatasetPdao, datasetDao, datasetId, updateModel));
    }

    if (DatasetSchemaUpdateUtils.hasRelationshipAdditions(updateModel)) {
      addStep(
          new DatasetSchemaUpdateAddRelationshipsPostgresStep(
              datasetTableDao, datasetId, relationshipDao, updateModel));
    }

    addStep(
        new DatasetSchemaUpdateResponseStep(
            datasetDao, datasetId, datasetService, updateModel, userRequest));

    addStep(new UnlockDatasetStep(datasetService, datasetId, false));
  }
}
