package bio.terra.service.snapshot.flight.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.Optional;
import java.util.UUID;

public class SnapshotDuosFlightUtils {

  public static DuosFirecloudGroupModel getFirecloudGroup(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP, DuosFirecloudGroupModel.class);
  }

  public static UUID getDuosFirecloudGroupId(DuosFirecloudGroupModel duosFirecloudGroup) {
    return Optional.ofNullable(duosFirecloudGroup).map(DuosFirecloudGroupModel::getId).orElse(null);
  }
}
