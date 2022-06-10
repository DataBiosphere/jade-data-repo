package bio.terra.service.dataset.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.AssetModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.InvalidAssetException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateDatasetAssetStep implements Step {

  private final ConfigurationService configService;
  private final AssetDao assetDao;
  private final DatasetService datasetService;

  public CreateDatasetAssetStep(
      AssetDao assetDao, ConfigurationService configService, DatasetService datasetService) {
    this.assetDao = assetDao;
    this.configService = configService;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    UUID datasetId =
        UUID.fromString(
            context.getInputParameters().get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    Dataset dataset = datasetService.retrieve(datasetId);
    FlightMap map = context.getWorkingMap();

    AssetModel assetModel =
        context.getInputParameters().get(JobMapKeys.REQUEST.getKeyName(), AssetModel.class);

    datasetService.validateDatasetAssetSpecification(dataset, assetModel);

    // get the dataset assets that already exist --asset name needs to be unique
    AssetSpecification newAssetSpecification = datasetService.getNewAssetSpec(dataset, assetModel);

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
    } catch (InvalidAssetException e) {
      FlightUtils.setErrorResponse(context, e.getMessage(), HttpStatus.BAD_REQUEST);
      map.put(DatasetWorkingMapKeys.ASSET_NAME_COLLISION, true);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }

    map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    if (map.get(DatasetWorkingMapKeys.ASSET_NAME_COLLISION, Boolean.class) == null) {
      UUID datasetId =
          UUID.fromString(
              context.getInputParameters().get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
      Dataset dataset = datasetService.retrieve(datasetId);

      AssetModel assetModel =
          context.getInputParameters().get(JobMapKeys.REQUEST.getKeyName(), AssetModel.class);
      // Search the Asset list in the dataset object to see if the asset you were trying to create
      // got created.
      Optional<AssetSpecification> assetSpecificationToDelete =
          dataset.getAssetSpecificationByName(assetModel.getName());
      // This only works if we are sure asset names are unique.
      // You cannot assume that the flight object created when the doStep was run is the same flight
      // object
      // when the undoStep is run.
      if (assetSpecificationToDelete.isPresent()) {
        // If the asset is found, then you get its id and call delete.
        assetDao.delete(assetSpecificationToDelete.get().getId());
      }
    }
    // Else, if the asset is not found, then you are done. It never got created.
    return StepResult.getStepResultSuccess();
  }
}
