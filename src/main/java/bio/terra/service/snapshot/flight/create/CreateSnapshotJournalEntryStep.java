package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordCreateEntryStep;
import bio.terra.service.journal.JournalService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;

public class CreateSnapshotJournalEntryStep extends JournalRecordCreateEntryStep {

  public CreateSnapshotJournalEntryStep(
      JournalService journalService, AuthenticatedUserRequest userReq) {
    super(journalService, userReq, IamResourceType.DATASNAPSHOT, "Created snapshot.", false);
  }

  @Override
  public UUID getResourceId(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
  }
}
