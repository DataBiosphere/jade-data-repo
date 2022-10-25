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

public class JournalCreateDeleteEntryStep implements Step {
  private final JournalService journalService;
  private final AuthenticatedUserRequest userReq;
  private final UUID resourceKey;
  private final IamResourceType resourceType;
  private final String note;

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
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    journalService.journalDelete(
        userReq,
        resourceKey,
        resourceType,
        note,
        getFlightInformationOfInterest(context.getInputParameters(), context));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
