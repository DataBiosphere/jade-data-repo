package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import java.util.List;
import java.util.UUID;
import liquibase.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertToPredictableFileIdsUpdateMissingMd5ChecksumsStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(ConvertToPredictableFileIdsUpdateMissingMd5ChecksumsStep.class);

  private final UUID datasetId;
  private final DatasetService datasetService;
  private final FireStoreDao fileDao;
  private final GcsProjectFactory gcsProjectFactory;

  public ConvertToPredictableFileIdsUpdateMissingMd5ChecksumsStep(
      UUID datasetId,
      DatasetService datasetService,
      FireStoreDao fileDao,
      GcsProjectFactory gcsProjectFactory) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.fileDao = fileDao;
    this.gcsProjectFactory = gcsProjectFactory;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);

    // Retrieve files with null MD5s
    List<FireStoreFile> noMd5Files = fileDao.retrieveAllWithEmptyField(dataset, "checksumMd5");
    if (noMd5Files.isEmpty()) {
      return StepResult.getStepResultSuccess();
    }
    logger.info("{} files will have md5s updated", noMd5Files.size());
    String projectId = dataset.getProjectResource().getGoogleProjectId();
    Storage storage = gcsProjectFactory.getStorage(projectId);

    // Add md5s
    noMd5Files.forEach(
        f -> {
          Blob sourceBlob = GcsPdao.getBlobFromGsPath(storage, f.getGspath(), projectId);
          f.checksumMd5(sourceBlob.getMd5ToHexString());
          if (StringUtil.isEmpty(f.getChecksumMd5())) {
            logger.warn("File {} has no MD5", f.getGspath());
          }
        });

    // Re-add files with md5s
    fileDao.upsertFileMetadata(dataset, noMd5Files);

    return StepResult.getStepResultSuccess();
  }
}
