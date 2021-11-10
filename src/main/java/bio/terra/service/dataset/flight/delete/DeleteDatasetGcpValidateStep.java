package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import java.util.UUID;

public class DeleteDatasetGcpValidateStep extends DeleteDatasetValidateStep {

  public DeleteDatasetGcpValidateStep(
      SnapshotDao snapshotDao,
      FireStoreDependencyDao dependencyDao,
      DatasetService datasetService,
      UUID datasetId) {
    super(snapshotDao, dependencyDao, datasetService, datasetId);
  }

  @Override
  boolean hasSnapshotReference(Dataset dataset, FlightContext context) throws InterruptedException {
    return dependencyDao.datasetHasSnapshotReference(dataset);
  }
}
