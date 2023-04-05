package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

public class ConvertToPredictableFileIdsUpdateFirestoreCollectionsStep implements Step {

  private final UUID datasetId;
  private final DatasetService datasetService;
  private final FireStoreDao fileDao;

  public ConvertToPredictableFileIdsUpdateFirestoreCollectionsStep(
      UUID datasetId, DatasetService datasetService, FireStoreDao fileDao) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.fileDao = fileDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);
    Map<UUID, UUID> oldToNewMappings =
        ConvertFileIdUtils.readFlightMappings(context.getWorkingMap());

    fileDao.moveFileMetadata(dataset, oldToNewMappings);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    // Reverse the id mapping to revert
    Map<UUID, UUID> newToOldMappings =
        ConvertFileIdUtils.readFlightMappings(context.getWorkingMap()).entrySet().stream()
            .collect(Collectors.toMap(Entry::getValue, Entry::getKey));

    fileDao.moveFileMetadata(dataset, newToOldMappings);
    return StepResult.getStepResultSuccess();
  }
}
