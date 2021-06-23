package bio.terra.service.dataset.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.AzureDataLocationSelector;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatasetGetOrCreateStorageAccountStep implements Step {
    private static Logger logger = LoggerFactory.getLogger(CreateDatasetGetOrCreateStorageAccountStep.class);
    private final ResourceService resourceService;
    private final DatasetRequestModel datasetRequestModel;
    private final AzureDataLocationSelector dataLocationSelector;

    public CreateDatasetGetOrCreateStorageAccountStep(ResourceService resourceService,
                                                      DatasetRequestModel datasetRequestModel,
                                                      AzureDataLocationSelector dataLocationSelector) {
        this.resourceService = resourceService;
        this.datasetRequestModel = datasetRequestModel;
        this.dataLocationSelector = dataLocationSelector;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        if (datasetRequestModel.getCloudPlatform() == CloudPlatform.AZURE) {
            logger.info("Creating a storage account for Azure backed dataset");
            FlightMap workingMap = context.getWorkingMap();
            BillingProfileModel profileModel = workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

            AzureStorageAccountResource storageAccount = resourceService.getOrCreateStorageAccount(
                DatasetJsonConversion.datasetRequestToDataset(datasetRequestModel),
                profileModel,
                context.getFlightId());
            workingMap.put(DatasetWorkingMapKeys.APPLICATION_DEPLOYMENT_RESOURCE_ID,
                storageAccount.getApplicationResource().getId());
            workingMap.put(DatasetWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_ID,
                storageAccount.getResourceId());
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

