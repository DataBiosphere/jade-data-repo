package bio.terra.service.snapshot.flight.create;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.journal.JournalService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class CreateSnapshotJournalEntryStep implements Step {
  private final AuthenticatedUserRequest userReq;
  JournalService journalService;

  public CreateSnapshotJournalEntryStep(
      JournalService journalService, AuthenticatedUserRequest userReq) {
    this.journalService = journalService;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotUUID = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    journalService.journalCreate(
        userReq,
        snapshotUUID,
        IamResourceType.DATASNAPSHOT,
        "Created snapshot.",
        getFlightInformationOfInterest(context));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
