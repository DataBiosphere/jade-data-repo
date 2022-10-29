package bio.terra.service.common;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Optional;
import java.util.UUID;

public class JournalCreateDeleteEntryStep implements Step {
  private final JournalService journalService;
  private final AuthenticatedUserRequest userReq;
  private final UUID resourceKey;
  private final IamResourceType resourceType;
  private final String note;
  private UUID journalEntryKey;

  public JournalCreateDeleteEntryStep(
      JournalService journalService,
      AuthenticatedUserRequest userRequest,
      UUID resourceKey,
      IamResourceType resourceType,
      String note) {
    this.journalService = journalService;
    this.userReq = userRequest;
    this.resourceKey = resourceKey;
    this.resourceType = resourceType;
    this.note = note;
    this.journalEntryKey = null;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    this.journalEntryKey =
        journalService.journalDelete(
            userReq, resourceKey, resourceType, note, getFlightInformationOfInterest(context));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Optional.ofNullable(this.journalEntryKey).ifPresent(this.journalService::removeJournalEntry);
    return StepResult.getStepResultSuccess();
  }
}
