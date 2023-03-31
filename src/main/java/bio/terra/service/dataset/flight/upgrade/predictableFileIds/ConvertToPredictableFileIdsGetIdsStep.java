package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FSFile;
import bio.terra.service.filedata.FileIdService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertToPredictableFileIdsGetIdsStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(ConvertToPredictableFileIdsGetIdsStep.class);

  private final UUID datasetId;
  private final DatasetService datasetService;
  private final FireStoreDao fileDao;
  private final FileIdService fileIdService;

  public ConvertToPredictableFileIdsGetIdsStep(
      UUID datasetId,
      DatasetService datasetService,
      FireStoreDao fileDao,
      FileIdService fileIdService) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.fileDao = fileDao;
    this.fileIdService = fileIdService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);

    // Retrieve existing file ids
    List<String> existingFileIds = fileDao.retrieveAllFileIds(dataset, false);
    List<FSFile> fsFiles = new ArrayList<>(existingFileIds.size());
    for (var fileIdBatch :
        ListUtils.partition(existingFileIds, FireStoreUtils.MAX_FIRESTORE_BATCH_SIZE)) {
      fsFiles.addAll(fileDao.batchRetrieveById(dataset, fileIdBatch, 0));
    }

    // Calculate new file ids (and store as strings to avoid serialization issues)
    Map<String, String> oldToNewMappings =
        fsFiles.stream()
            .map(
                f ->
                    Map.entry(
                        f.getFileId().toString(),
                        fileIdService.calculateFileId(true, f).toString()))
            .filter(e -> !Objects.equals(e.getKey(), e.getValue()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    context.getWorkingMap().put(ConvertFileIdUtils.FILE_ID_MAPPINGS_FIELD, oldToNewMappings);
    return StepResult.getStepResultSuccess();
  }
}
