package bio.terra.service.common;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public abstract class JournalRecordCreateEntryStep implements Step {
  private final JournalService journalService;
  private final AuthenticatedUserRequest userReq;
  private final IamResourceType resourceType;
  private final String note;
  private final boolean clearHistory;

  public JournalRecordCreateEntryStep(
      JournalService journalService,
      AuthenticatedUserRequest userRequest,
      IamResourceType resourceType,
      String note,
      boolean clearHistory) {
    this.journalService = journalService;
    this.userReq = userRequest;
    this.resourceType = resourceType;
    this.note = note;
    this.clearHistory = clearHistory;
  }

  public abstract UUID getResourceId(FlightContext context) throws InterruptedException;

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    journalService.recordCreate(
        userReq,
        getResourceId(context),
        resourceType,
        note,
        getFlightInformationOfInterest(context),
        clearHistory);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    String flightId = context.getFlightId();
    journalService.removeJournalEntriesByFlightId(flightId);
    return StepResult.getStepResultSuccess();
  }
}
