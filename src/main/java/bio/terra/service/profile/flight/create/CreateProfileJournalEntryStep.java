package bio.terra.service.profile.flight.create;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateProfileJournalEntryStep implements Step {
  private final JournalService journalService;
  private final AuthenticatedUserRequest userReq;
  private final BillingProfileRequestModel billingReq;

  public CreateProfileJournalEntryStep(
      JournalService journalService,
      AuthenticatedUserRequest user,
      BillingProfileRequestModel request) {
    this.journalService = journalService;
    this.userReq = user;
    this.billingReq = request;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    journalService.journalCreate(
        userReq,
        billingReq.getId(),
        IamResourceType.SPEND_PROFILE,
        "Billing profile created.",
        getFlightInformationOfInterest(context.getInputParameters(), context),
        true);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
