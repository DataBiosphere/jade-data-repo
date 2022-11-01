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
import java.util.Optional;
import java.util.UUID;

public class CreateProfileJournalEntryStep implements Step {
  private final JournalService journalService;
  private final AuthenticatedUserRequest userReq;
  private final BillingProfileRequestModel billingReq;
  private UUID journalEntryKey;

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
    this.journalEntryKey =
        journalService.recordCreate(
            userReq,
            billingReq.getId(),
            IamResourceType.SPEND_PROFILE,
            "Billing profile created.",
            getFlightInformationOfInterest(context),
            true);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Optional.ofNullable(this.journalEntryKey).ifPresent(this.journalService::removeJournalEntry);
    return StepResult.getStepResultSuccess();
  }
}
