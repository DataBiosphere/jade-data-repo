package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;

public class CreateSnapshotValidateAssetStep implements Step {

  private final SnapshotService snapshotService;
  private final SnapshotRequestModel snapshotReq;
  private final int sourceIndex;

  public CreateSnapshotValidateAssetStep(
      SnapshotService snapshotService, SnapshotRequestModel snapshotReq, int sourceIndex) {
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
    this.sourceIndex = sourceIndex;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    /*
     * get dataset
     * get dataset asset list
     * get snapshot asset name
     * check that snapshot asset exists
     */
    Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());
    SnapshotSource snapshotSource = snapshot.getSnapshotSources().get(sourceIndex);

    Dataset dataset = snapshotSource.getDataset();
    List<String> datasetAssetNames =
        dataset.getAssetSpecifications().stream()
            .map(AssetSpecification::getName)
            .collect(Collectors.toList());
    String assetName = snapshotSource.getAssetSpecification().getName();
    if (!datasetAssetNames.contains(assetName)) {
      String message =
          String.format(
              "Mismatched asset name: '%s' is not an asset in the asset list for dataset '%s'."
                  + "Asset list is '%s'",
              assetName, dataset.getName(), String.join("', '", datasetAssetNames));
      FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
