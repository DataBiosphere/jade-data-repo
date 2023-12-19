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

public class JournalRecordUpdateEntryStep implements Step {
  private final JournalService journalService;
  private final AuthenticatedUserRequest userReq;
  private final UUID resourceKey;
  private final IamResourceType resourceType;
  private String note;
  private final boolean includeFlightIdInNote;

  public JournalRecordUpdateEntryStep(
      JournalService journalService,
      AuthenticatedUserRequest userRequest,
      UUID resourceKey,
      IamResourceType resourceType,
      String note,
      boolean includeFlightIdInNote) {
    this.journalService = journalService;
    this.userReq = userRequest;
    this.resourceKey = resourceKey;
    this.resourceType = resourceType;
    this.note = note;
    this.includeFlightIdInNote = includeFlightIdInNote;
  }

  public JournalRecordUpdateEntryStep(
      JournalService journalService,
      AuthenticatedUserRequest userRequest,
      UUID resourceKey,
      IamResourceType resourceType,
      String note) {
    this(journalService, userRequest, resourceKey, resourceType, note, false);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    if (includeFlightIdInNote) {
      note = note + " (job id = " + context.getFlightId() + ")";
    }
    journalService.recordUpdate(
        userReq, resourceKey, resourceType, note, getFlightInformationOfInterest(context));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    String flightId = context.getFlightId();
    journalService.removeJournalEntriesByFlightId(flightId);
    return StepResult.getStepResultSuccess();
  }
}
