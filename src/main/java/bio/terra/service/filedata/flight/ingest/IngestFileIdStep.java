package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;

/**
 * The sole purpose of this step is to allocate the file object id and store it in the working map.
 * That allows the later steps to be simpler. Implementations of file storage on other platforms may
 * be quite different, so we do not want to push this out of the firestore-specific code and into
 * the common code.
 */
public class IngestFileIdStep extends DefaultUndoStep {

  public IngestFileIdStep() {}

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    String fileId = UUID.randomUUID().toString();
    workingMap.put(FileMapKeys.FILE_ID, fileId);

    return StepResult.getStepResultSuccess();
  }
}
