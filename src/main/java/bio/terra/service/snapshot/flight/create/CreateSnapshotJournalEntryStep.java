package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordCreateEntryStep;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import java.util.UUID;

public class CreateSnapshotJournalEntryStep extends JournalRecordCreateEntryStep {
  private final UUID snapshotId;

  public CreateSnapshotJournalEntryStep(
      JournalService journalService, AuthenticatedUserRequest userReq, UUID snapshotId) {
    super(journalService, userReq, IamResourceType.DATASNAPSHOT, "Created snapshot.", false);
    this.snapshotId = snapshotId;
  }

  @Override
  public UUID getResourceId(FlightContext context) throws InterruptedException {
    return snapshotId;
  }
}
