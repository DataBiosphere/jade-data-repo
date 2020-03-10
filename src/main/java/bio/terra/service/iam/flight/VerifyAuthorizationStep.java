package bio.terra.service.iam.flight;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

/**
 * Shareable step for running authz checks inside flights
 */
public class VerifyAuthorizationStep implements Step {
    private final IamService iamService;
    private final IamResourceType iamResourceType;
    private final String resourceId;
    private final IamAction action;

    public VerifyAuthorizationStep(IamService iamService,
                                   IamResourceType iamResourceType,
                                   String resourceId,
                                   IamAction action) {
        this.iamService = iamService;
        this.iamResourceType = iamResourceType;
        this.resourceId = resourceId;
        this.action = action;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap parameterMap = context.getInputParameters();
        AuthenticatedUserRequest userReq =
            parameterMap.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        iamService.verifyAuthorization(userReq, iamResourceType, resourceId, action);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Nothing to undo
        return StepResult.getStepResultSuccess();
    }
}
