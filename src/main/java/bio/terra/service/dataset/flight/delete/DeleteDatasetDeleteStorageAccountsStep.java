package bio.terra.service.dataset.flight.delete;

import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DeleteDatasetDeleteStorageAccountsStep implements Step {
    private static Logger logger = LoggerFactory.getLogger(DeleteDatasetDeleteStorageAccountsStep.class);
    private final ResourceService resourceService;
    private final DatasetService datasetService;
    private final UUID datasetId;

    public DeleteDatasetDeleteStorageAccountsStep(ResourceService resourceService,
                                                  DatasetService datasetService,
                                                  UUID datasetId) {
        this.resourceService = resourceService;
        this.datasetService = datasetService;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        Dataset dataset = datasetService.retrieve(datasetId);
        boolean isAzureDataset = dataset.getStorage().stream()
            .anyMatch(s -> s.getCloudPlatform() == CloudPlatform.AZURE);
        if (isAzureDataset) {
            logger.info("Deleting a storage account for Azure backed dataset");
            resourceService.deleteStorageAccount(dataset, context.getFlightId());
        } else {
            logger.info("Not an Azure backed dataset so no action to take");
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // Leaving artifacts on undo
        return StepResult.getStepResultSuccess();
    }

}

