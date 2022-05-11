package bio.terra.service.dataset.flight.update;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
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
    BigQueryDatasetPdao bigQueryDatasetPdao = appContext.getBean(BigQueryDatasetPdao.class);

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
    }

    addStep(new UnlockDatasetStep(datasetService, datasetId, false));
  }
}
