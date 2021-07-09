package bio.terra.service.dataset.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.common.Relationship;
import bio.terra.model.AssetModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.AssetDao;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidAssetException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private Dataset getDataset(FlightContext context) {
    // Use the dataset id to fetch the Dataset object
    UUID datasetId =
        UUID.fromString(
            context.getInputParameters().get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    return datasetService.retrieve(datasetId);
  }

  private AssetSpecification getNewAssetSpec(FlightContext context, Dataset dataset) {
    // get Asset Model and convert it to a spec
    AssetModel assetModel =
        context.getInputParameters().get(JobMapKeys.REQUEST.getKeyName(), AssetModel.class);

    List<DatasetTable> datasetTables = dataset.getTables();
    Map<String, Relationship> relationshipMap = new HashMap<>();
    Map<String, DatasetTable> tablesMap = new HashMap<>();

    datasetTables.forEach(datasetTable -> tablesMap.put(datasetTable.getName(), datasetTable));

    List<Relationship> datasetRelationships = dataset.getRelationships();

    datasetRelationships.forEach(
        relationship -> relationshipMap.put(relationship.getName(), relationship));
    AssetSpecification assetSpecification =
        DatasetJsonConversion.assetModelToAssetSpecification(
            assetModel, tablesMap, relationshipMap);
    return assetSpecification;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // TODO: Asset columns and tables need to match things in the dataset schema
    Dataset dataset = getDataset(context);
    FlightMap map = context.getWorkingMap();

    // get the dataset assets that already exist --asset name needs to be unique
    AssetSpecification newAssetSpecification = getNewAssetSpec(context, dataset);

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
      assetDao.create(newAssetSpecification, dataset.getId());
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
      Dataset dataset = getDataset(context);
      // Search the Asset list in the dataset object to see if the asset you were trying to create
      // got created.
      AssetSpecification newAssetSpecification = getNewAssetSpec(context, dataset);
      Optional<AssetSpecification> assetSpecificationToDelete =
          dataset.getAssetSpecificationByName(newAssetSpecification.getName());
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
