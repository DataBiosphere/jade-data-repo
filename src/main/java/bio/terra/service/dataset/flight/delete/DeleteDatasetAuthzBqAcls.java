package bio.terra.service.dataset.flight.delete;

import bio.terra.model.DatasetModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.IamService;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class DeleteDatasetAuthzBqAcls implements Step {
    private final IamService sam;
    private final DatasetService datasetService;
    private final GoogleResourceService resourceService;
    private final UUID datasetId;
    private final AuthenticatedUserRequest userReq;

    public DeleteDatasetAuthzBqAcls(IamService sam,
                                    DatasetService datasetService,
                                    GoogleResourceService resourceService,
                                    UUID datasetId,
                                    AuthenticatedUserRequest userReq) {
        this.sam = sam;
        this.datasetService = datasetService;
        this.resourceService = resourceService;
        this.datasetId = datasetId;
        this.userReq = userReq;
    }

    private static Logger logger = LoggerFactory.getLogger(DeleteDatasetAuthzBqAcls.class);

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        Dataset dataset = datasetService.retrieve(datasetId);
        DatasetModel datasetModel = datasetService.retrieveModel(dataset);

        Map<IamRole, String> policies = sam.retrievePolicyEmails(userReq, IamResourceType.DATASET, datasetId);

        // The underlying service provides retries so we do not need to retry this operation
        resourceService.revokePoliciesBqJobUser(datasetModel.getDataProject(), policies.values());

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo delete
        logger.warn("Trying to undo clear ACLs for dataset " + datasetId.toString());
        return StepResult.getStepResultSuccess();
    }
}
