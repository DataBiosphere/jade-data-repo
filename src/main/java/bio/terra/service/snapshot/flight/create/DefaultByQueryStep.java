package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;

public abstract class DefaultByQueryStep implements Step {

  SnapshotRequestModel getByQueryRequestModel(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    if (workingMap.containsKey(SnapshotWorkingMapKeys.BY_QUERY_SNAPSHOT_REQUEST_MODEL)) {
      return workingMap.get(
          SnapshotWorkingMapKeys.BY_QUERY_SNAPSHOT_REQUEST_MODEL, SnapshotRequestModel.class);
    } else {
      return context
          .getInputParameters()
          .get(JobMapKeys.REQUEST.getKeyName(), SnapshotRequestModel.class);
    }
  }
}
