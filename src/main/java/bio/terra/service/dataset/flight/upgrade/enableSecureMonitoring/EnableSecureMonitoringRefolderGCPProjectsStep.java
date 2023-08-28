package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnableSecureMonitoringRefolderGCPProjectsStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(EnableSecureMonitoringRefolderGCPProjectsStep.class);

  private final Dataset dataset;
  private final BufferService bufferService;
  private final SnapshotService snapshotService;
  private final AuthenticatedUserRequest userRequest;

  public EnableSecureMonitoringRefolderGCPProjectsStep(
      Dataset dataset,
      SnapshotService snapshotService,
      BufferService bufferService,
      AuthenticatedUserRequest userRequest) {
    this.dataset = dataset;
    this.snapshotService = snapshotService;
    this.bufferService = bufferService;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    List<String> projectsToRefolder = new ArrayList<>();
    DatasetSummary datasetSummary = dataset.getDatasetSummary();
    projectsToRefolder.add(datasetSummary.getDataProject());
    projectsToRefolder.addAll(
        snapshotService.enumerateGcpProjectsForSourceDataset(dataset.getId(), userRequest));
    // We expect to be able to re-run this command multiple times without error
    // It should just pass over the project if it's already in the secure folder
    projectsToRefolder.forEach(
        projectId -> {
          try {
            bufferService.refolderProjectToSecureFolder(projectId);
          } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(
                "Dataset was not updated - failed to refolder project " + projectId, e);
          }
        });

    return StepResult.getStepResultSuccess();
  }
}
