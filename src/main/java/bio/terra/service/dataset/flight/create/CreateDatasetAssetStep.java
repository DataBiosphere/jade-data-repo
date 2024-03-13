package bio.terra.service.dataset.flight.create;

import bio.terra.common.BaseStep;
import bio.terra.common.StepInput;
import bio.terra.common.StepOutput;
import bio.terra.model.AssetModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.InvalidAssetException;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateDatasetAssetStep extends BaseStep {

  private final ConfigurationService configService;
  private final AssetDao assetDao;
  private final DatasetService datasetService;

  @StepInput UUID datasetId;
  @StepInput AssetModel request;

  /** If true, an asset was created during doStep(), and needs to be deleted during undoStep(). */
  @StepInput @StepOutput boolean assetCreated;

  public CreateDatasetAssetStep(
      AssetDao assetDao, ConfigurationService configService, DatasetService datasetService) {
    this.assetDao = assetDao;
    this.configService = configService;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult perform() {
    Dataset dataset = datasetService.retrieve(datasetId);

    dataset.validateDatasetAssetSpecification(request);

    // get the dataset assets that already exist --asset name needs to be unique
    AssetSpecification newAssetSpecification = dataset.getNewAssetSpec(request);

    // add a fault that forces an exception to make sure the undo works
    try {
      configService.fault(
          ConfigEnum.CREATE_ASSET_FAULT,
          () -> {
            throw new RuntimeException("fault insertion");
          });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      assetDao.create(newAssetSpecification, datasetId);
      assetCreated = true;
    } catch (InvalidAssetException e) {
      setErrorResponse(e.getMessage(), HttpStatus.BAD_REQUEST);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }

    statusCode = HttpStatus.CREATED;
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undo() {
    if (assetCreated) {
      Dataset dataset = datasetService.retrieve(datasetId);

      // Search the Asset list in the dataset object to see if the asset you were trying to create
      // got created.
      Optional<AssetSpecification> assetSpecificationToDelete =
          dataset.getAssetSpecificationByName(request.getName());
      // This only works if we are sure asset names are unique.
      // You cannot assume that the flight object created when the doStep was run is the same flight
      // object
      // when the undoStep is run.
      // If the asset is found, then you get its id and call delete.
      assetSpecificationToDelete.map(AssetSpecification::getId).ifPresent(assetDao::delete);
    }
    // Else, if the asset is not found, then you are done. It never got created.
    return StepResult.getStepResultSuccess();
  }
}
