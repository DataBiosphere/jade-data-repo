package bio.terra.service.dataset.flight.create;

import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.SamClientService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class CreateDatasetAuthzResource implements Step {
    private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzResource.class);

    private SamClientService sam;
    private BigQueryPdao bigQueryPdao;
    private DatasetService datasetService;
    private AuthenticatedUserRequest userReq;

    public CreateDatasetAuthzResource(
        SamClientService sam,
        BigQueryPdao bigQueryPdao,
        DatasetService datasetService,
        AuthenticatedUserRequest userReq) {
        this.sam = sam;
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
        this.userReq = userReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        Dataset dataset = datasetService.retrieve(datasetId);
        List<String> policyEmails = sam.createDatasetResource(userReq, datasetId);
        bigQueryPdao.grantReadAccessToDataset(dataset, policyEmails);
        // TODO: on file ingest these policies also need to be added as readers
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        try {
            sam.deleteDatasetResource(userReq, datasetId);
        } catch (UnauthorizedException ex) {
            // suppress exception
            logger.error("NEEDS CLEANUP: delete sam resource for dataset " + datasetId.toString(), ex);
        } catch (NotFoundException ex) {
            // if the SAM resource is not found, then it was likely not created -- continue undoing
        }
        return StepResult.getStepResultSuccess();
    }
}
