package bio.terra.service.upgrade.flight;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class MigrateDatabaseAuthStep implements Step {
    private final AuthenticatedUserRequest user;
    private final IamService iamService;
    private final ApplicationConfiguration applicationConfiguration;


    public MigrateDatabaseAuthStep(IamService iamService,
                                   ApplicationConfiguration applicationConfiguration,
                                   AuthenticatedUserRequest user) {
        this.iamService = iamService;
        this.applicationConfiguration = applicationConfiguration;
        this.user = user;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        // assert user is an admin by checking for the configure action
        iamService.verifyAuthorization(
            user,
            IamResourceType.DATAREPO,
            applicationConfiguration.getResourceId(),
            IamAction.CONFIGURE);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        return StepResult.getStepResultSuccess();
    }
}
