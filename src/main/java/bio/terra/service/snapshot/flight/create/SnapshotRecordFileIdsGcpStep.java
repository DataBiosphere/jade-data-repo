package bio.terra.service.snapshot.flight.create;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import java.util.List;
import java.util.UUID;

public class SnapshotRecordFileIdsGcpStep extends SnapshotRecordFileIdsStep {

  private final FireStoreDao fireStoreDao;

  public SnapshotRecordFileIdsGcpStep(
      SnapshotService snapshotService,
      DatasetService datasetService,
      DrsIdService drsIdService,
      DrsService drsService,
      FireStoreDao fireStoreDao,
      UUID snapshotId) {
    super(snapshotService, datasetService, drsIdService, drsService, snapshotId);
    this.fireStoreDao = fireStoreDao;
  }

  @Override
  List<String> getFileIds(FlightContext context, Snapshot snapshot) throws InterruptedException {
    return fireStoreDao.retrieveAllFileIds(snapshot, true);
  }
}
