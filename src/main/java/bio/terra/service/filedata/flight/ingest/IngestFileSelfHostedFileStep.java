package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.FileLoadModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestFileSelfHostedFileStep implements Step {
  private final Dataset dataset;
  private final GcsPdao gcsPdao;

  public IngestFileSelfHostedFileStep(Dataset dataset, GcsPdao gcsPdao) {
    this.dataset = dataset;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap inputParameters = context.getInputParameters();
    FileLoadModel fileLoadModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

    FlightMap workingMap = context.getWorkingMap();
    String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      FSFileInfo fsFileInfo =
          gcsPdao.copySelfHostedFile(
              fileLoadModel, fileId, dataset.getProjectResource().getGoogleProjectId());
      workingMap.put(FileMapKeys.FILE_INFO, fsFileInfo);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
