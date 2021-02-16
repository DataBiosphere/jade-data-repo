package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.iam.IamRole;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class CreateDatasetAuthzIamPolicyStep implements Step {
    private static final Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzIamPolicyStep.class);

    private final IamProviderInterface iamClient;
    private final AuthenticatedUserRequest userReq;

    public CreateDatasetAuthzIamPolicyStep(
        IamProviderInterface iamClient,
        AuthenticatedUserRequest userReq) {
        this.iamClient = iamClient;
        this.userReq = userReq;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        Map<IamRole, String> policyEmails = iamClient.syncDatasetResourcePolicies(userReq, datasetId);
        workingMap.put(DatasetWorkingMapKeys.POLICY_EMAILS, policyEmails);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        return StepResult.getStepResultSuccess();
    }
}
