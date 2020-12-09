package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateProfileUpdateGCloudProject implements Step {
    private static final Logger logger = LoggerFactory.getLogger(UpdateProfileUpdateGCloudProject.class);
    private final GoogleProjectService googleProjectService;

    public UpdateProfileUpdateGCloudProject(GoogleProjectService projectService) {
        this.googleProjectService = projectService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        // Check if billing profile id is diff from existing
        FlightMap workingMap = context.getWorkingMap();
        BillingProfileModel existingBillingProfileModel =
            workingMap.get(JobMapKeys.REVERT_TO.getKeyName(), BillingProfileModel.class);
        BillingProfileModel newBillingProfileModel =
            workingMap.get(JobMapKeys.RESPONSE.getKeyName(), BillingProfileModel.class);
        // maybe I should check the actual google project billing account instead?
        // This is an okay start
        if (existingBillingProfileModel.getBillingAccountId() != newBillingProfileModel.getBillingAccountId()) {
            googleProjectService.updateProjectsBillingAccount(newBillingProfileModel);
        } else {
            logger.info("Billing profile id already set to {}", newBillingProfileModel.getBillingAccountId());
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // We'll go ahead and update every project, even if it wasn't changed in "do" step.
        FlightMap workingMap = context.getWorkingMap();
        BillingProfileModel existingBillingProfileModel =
            workingMap.get(JobMapKeys.REVERT_TO.getKeyName(), BillingProfileModel.class);

        googleProjectService.updateProjectsBillingAccount(existingBillingProfileModel);

        return StepResult.getStepResultSuccess();
    }
}

