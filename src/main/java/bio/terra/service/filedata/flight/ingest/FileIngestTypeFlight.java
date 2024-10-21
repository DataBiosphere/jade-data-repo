package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRule;

public abstract class FileIngestTypeFlight extends Flight {

  public FileIngestTypeFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);
  }

  /**
   * Order depends on how the file id is obtained. If we are generating the file predictably, we
   * determine it when ingesting the file (by reading the MD5) so we need to run that first.
   * Otherwise, we need to first perform the directory creation step and then the physical copy of
   * the file
   */
  protected void addFileCopyAndDirectoryRecordStepsGcp(
      FireStoreDao fileDao,
      GcsPdao gcsPdao,
      ConfigurationService configService,
      Dataset dataset,
      RetryRule fileSystemRetry) {
    if (dataset.hasPredictableFileIds()) {
      addStep(new IngestFilePrimaryDataStep(dataset, gcsPdao, configService), fileSystemRetry);
      addStep(new IngestFileDirectoryStep(fileDao, dataset), fileSystemRetry);
    } else {
      addStep(new IngestFileDirectoryStep(fileDao, dataset), fileSystemRetry);
      addStep(new IngestFilePrimaryDataStep(dataset, gcsPdao, configService), fileSystemRetry);
    }
  }

  protected void addFileCopyAndDirectoryRecordStepsAzure(
      AzureBlobStorePdao azureBlobStorePdao,
      ConfigurationService configService,
      TableDao azureTableDao,
      AuthenticatedUserRequest userReq,
      Dataset dataset,
      RetryRule fileSystemRetry) {
    if (dataset.hasPredictableFileIds()) {
      addStep(
          new IngestFileAzurePrimaryDataStep(dataset, azureBlobStorePdao, configService, userReq));
      addStep(new IngestFileAzureDirectoryStep(azureTableDao, dataset), fileSystemRetry);
    } else {
      addStep(new IngestFileAzureDirectoryStep(azureTableDao, dataset), fileSystemRetry);
      addStep(
          new IngestFileAzurePrimaryDataStep(dataset, azureBlobStorePdao, configService, userReq));
    }
  }
}
