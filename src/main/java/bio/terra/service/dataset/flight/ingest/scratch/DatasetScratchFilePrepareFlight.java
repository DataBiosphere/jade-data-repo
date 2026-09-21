package bio.terra.service.dataset.flight.ingest.scratch;

import static bio.terra.common.FlightUtils.getDefaultRandomBackoffRetryRule;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.CommonExceptions;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.flight.ingest.CreateBucketForBigQueryScratchStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

/** Create the scratch bucket/storage account to use when ingesting data from a payload */
public class DatasetScratchFilePrepareFlight extends Flight {

  public DatasetScratchFilePrepareFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // get the required daos to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;

    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    DatasetService datasetService = appContext.getBean(DatasetService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);

    Dataset dataset =
        datasetService.retrieve(
            UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class)));

    CloudPlatformWrapper cloudPlatform =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());

    if (cloudPlatform.isGcp()) {
      addStep(
          new CreateBucketForBigQueryScratchStep(resourceService, datasetService),
          getDefaultRandomBackoffRetryRule(appConfig.getMaxStairwayThreads()));
      addStep(new CreateScratchFileForGCPStep());
    } else if (cloudPlatform.isAzure()) {
      throw CommonExceptions.AZURE_NOT_SUPPORTED;
    }
  }
}
