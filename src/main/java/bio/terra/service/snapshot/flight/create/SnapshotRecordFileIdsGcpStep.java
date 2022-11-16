package bio.terra.service.snapshot.flight.create;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import java.util.List;
import java.util.UUID;

public class SnapshotRecordFileIdsGcpStep extends SnapshotRecordFileIdsStep {

  private final FireStoreDependencyDao fireStoreDao;

  public SnapshotRecordFileIdsGcpStep(
      SnapshotService snapshotService,
      DatasetService datasetService,
      DrsIdService drsIdService,
      DrsService drsService,
      FireStoreDependencyDao fireStoreDao) {
    super(snapshotService, datasetService, drsIdService, drsService);
    this.fireStoreDao = fireStoreDao;
  }

  @Override
  List<String> getFileIds(FlightContext context, Dataset dataset, UUID snapshotId)
      throws InterruptedException {
    return fireStoreDao.getDatasetSnapshotFileIds(dataset, snapshotId.toString());
  }
}
