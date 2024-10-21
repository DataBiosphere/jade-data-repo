package bio.terra.service.admin.flight;

import bio.terra.model.DrsAliasModel;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;

public class DrsAliasVerifyStep extends DefaultUndoStep {
  private final DrsIdService drsIdService;
  private final List<DrsAliasModel> aliases;

  public DrsAliasVerifyStep(DrsIdService drsIdService, List<DrsAliasModel> aliases) {
    this.drsIdService = drsIdService;
    this.aliases = aliases;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    // Fail if any of the aliases are valid TDR DRS IDs
    boolean anyAliasesAreValid =
        aliases.stream().anyMatch(a -> drsIdService.isValidObjectId(a.getAliasDrsObjectId()));
    if (anyAliasesAreValid) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new IllegalArgumentException(
              "One or more aliases are valid TDR DRS IDs which is not allowed"));
    }

    boolean anyTdrIdsAreInvalid =
        aliases.stream().anyMatch(a -> !drsIdService.isValidObjectId(a.getTdrDrsObjectId()));
    if (anyTdrIdsAreInvalid) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new IllegalArgumentException("One or more TDR Ids are invalid"));
    }
    return StepResult.getStepResultSuccess();
  }
}
