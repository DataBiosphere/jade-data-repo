package bio.terra.service.snapshot.flight.create;

import bio.terra.common.BaseStep;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;

public class CreateSnapshotValidateAssetStep extends BaseStep {

  private DatasetService datasetService;
  private SnapshotService snapshotService;
  private SnapshotRequestModel snapshotReq;

  public CreateSnapshotValidateAssetStep(
      DatasetService datasetService,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq) {
    this.datasetService = datasetService;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult perform() {
    /*
     * get dataset
     * get dataset asset list
     * get snapshot asset name
     * check that snapshot asset exists
     */
    Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());

    for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
      Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
      List<String> datasetAssetNamesList =
          dataset.getAssetSpecifications().stream()
              .map(assetSpec -> assetSpec.getName())
              .collect(Collectors.toList());
      String snapshotSourceAssetName = snapshotSource.getAssetSpecification().getName();
      if (!datasetAssetNamesList.contains(snapshotSourceAssetName)) {
        String datasetAssetNames = String.join("', '", datasetAssetNamesList);
        String message =
            String.format(
                "Mismatched asset name: '%s' is not an asset in the asset list for dataset '%s'."
                    + "Asset list is '%s'",
                snapshotSourceAssetName, dataset.getName(), datasetAssetNames);
        setErrorResponse(message, HttpStatus.BAD_REQUEST);
        return new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
