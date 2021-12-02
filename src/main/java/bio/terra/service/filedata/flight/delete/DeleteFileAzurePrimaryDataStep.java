package bio.terra.service.filedata.flight.delete;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteFileAzurePrimaryDataStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteFileAzurePrimaryDataStep.class);

  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AuthenticatedUserRequest userRequest;

  public DeleteFileAzurePrimaryDataStep(
      AzureBlobStorePdao azureBlobStorePdao, AuthenticatedUserRequest userRequest) {
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    FireStoreFile fireStoreFile = workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class);
    if (fireStoreFile != null) {
      azureBlobStorePdao.deleteFile(fireStoreFile, userRequest);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // No undo is possible - the file either still exists or it doesn't
    return StepResult.getStepResultSuccess();
  }
}
