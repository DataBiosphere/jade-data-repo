package bio.terra.service.snapshot.flight.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;

public class SnapshotDuosFlightUtils {

  public static DuosFirecloudGroupModel getFirecloudGroup(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP, DuosFirecloudGroupModel.class);
  }
}
