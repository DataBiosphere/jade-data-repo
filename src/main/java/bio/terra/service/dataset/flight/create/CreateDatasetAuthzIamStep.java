package bio.terra.service.dataset.flight.create;

import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class CreateDatasetAuthzIamStep implements Step {
    private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzIamStep.class);

    private final IamProviderInterface iamClient;
    private final AuthenticatedUserRequest userReq;

    public CreateDatasetAuthzIamStep(
        IamProviderInterface iamClient,
        AuthenticatedUserRequest userReq) {
        this.iamClient = iamClient;
        this.userReq = userReq;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        List<String> policyEmails = iamClient.createDatasetResource(userReq, datasetId);
        workingMap.put(DatasetWorkingMapKeys.POLICY_EMAILS, policyEmails);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        try {
            iamClient.deleteDatasetResource(userReq, datasetId);
        } catch (UnauthorizedException ex) {
            // suppress exception
            logger.error("NEEDS CLEANUP: delete sam resource for dataset " + datasetId.toString(), ex);
        } catch (NotFoundException ex) {
            // if the SAM resource is not found, then it was likely not created -- continue undoing
        }
        return StepResult.getStepResultSuccess();
    }
}
