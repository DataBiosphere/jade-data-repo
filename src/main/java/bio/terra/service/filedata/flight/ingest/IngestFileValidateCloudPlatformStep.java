package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.exception.NotImplementedException;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestFileValidateCloudPlatformStep implements Step {

    private final Logger logger = LoggerFactory.getLogger(IngestFileValidateCloudPlatformStep.class);
    private final Dataset dataset;

    public IngestFileValidateCloudPlatformStep(Dataset dataset) {
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        BillingProfileModel profileModel = workingMap.get(
            ProfileMapKeys.PROFILE_MODEL,
            BillingProfileModel.class);
        if (profileModel.getCloudPlatform() == CloudPlatform.AZURE) {
            logger.warn("Attempted to ingest file into Azure. Tenant: {}, Subscription: {}, Resource Group: {}",
                profileModel.getTenantId(), profileModel.getSubscriptionId(), profileModel.getResourceGroupName());
            NotImplementedException ex = new NotImplementedException("Can't ingest files into an Azure dataset yet.");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }
        CloudPlatform cloudPlatform = dataset.getDatasetSummary().getStorageCloudPlatform();

        if (profileModel.getCloudPlatform() != cloudPlatform) {
            logger.warn(
                "The storage cloud platform of this dataset ({}) does not match the billing cloud platform  ({})",
                cloudPlatform,
                profileModel.getCloudPlatform()
                );
            IngestFailureException ex = new IngestFailureException(
                String.format("Can't ingest files into a %s storage using %s billing profile.",
                    cloudPlatform,
                    profileModel.getCloudPlatform()));
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // This step has no side effects
        return StepResult.getStepResultSuccess();
    }
}
