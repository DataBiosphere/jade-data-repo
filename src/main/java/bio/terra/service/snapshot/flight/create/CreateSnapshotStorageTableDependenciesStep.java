package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.FileLoadModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FSItem;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.data.tables.models.TableServiceException;

public class CreateSnapshotStorageTableDependenciesStep implements Step {
  private final TableDao tableDao;

  public CreateSnapshotStorageTableDependenciesStep(TableDao tableDao) {
    this.tableDao = tableDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    AzureStorageAuthInfo storageAuthInfo =
        FlightUtils.getContextValue(
            context, FileMapKeys.STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);


    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {

    return StepResult.getStepResultSuccess();
  }
}
