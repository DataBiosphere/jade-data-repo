package bio.terra.service.dataset.flight.create;

import bio.terra.model.DatasetModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class CreateDatasetAuthzBqJobUserStep implements Step {
    private static final Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzBqJobUserStep.class);

    private final DatasetService datasetService;
    private final GoogleResourceService resourceService;

    public CreateDatasetAuthzBqJobUserStep(
        IamProviderInterface iamClient,
        BigQueryPdao bigQueryPdao,
        DatasetService datasetService,
        AuthenticatedUserRequest userReq,
        GoogleResourceService resourceService) {
        this.datasetService = datasetService;
        this.resourceService = resourceService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        List<String> policyEmails = workingMap.get(DatasetWorkingMapKeys.POLICY_EMAILS, List.class);
        Dataset dataset = datasetService.retrieve(datasetId);
        DatasetModel datasetModel = datasetService.retrieveModel(dataset);

        // The underlying service provides retries so we do not need to retry this operation
        resourceService.grantPoliciesBqJobUser(datasetModel, policyEmails);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // TODO:
        return StepResult.getStepResultSuccess();
    }
}
