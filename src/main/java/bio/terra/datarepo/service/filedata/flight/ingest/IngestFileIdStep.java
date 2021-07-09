package bio.terra.datarepo.service.filedata.flight.ingest;

import bio.terra.datarepo.service.configuration.ConfigEnum;
import bio.terra.datarepo.service.configuration.ConfigurationService;
import bio.terra.datarepo.service.dataset.exception.DatasetLockException;
import bio.terra.datarepo.service.filedata.flight.FileMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// The sole purpose of this step is to allocate the file object id
// and store it in the working map. That allows the later steps to
// be simpler. Implementations of file storage on other platforms may
// be quite different, so we do not want to push this out of the firestore-specific
// code and into the common code.
public class IngestFileIdStep implements Step {

  private static Logger logger = LoggerFactory.getLogger(IngestFileIdStep.class);

  private ConfigurationService configService;

  public IngestFileIdStep(ConfigurationService configService) {
    this.configService = configService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    String fileId = UUID.randomUUID().toString();
    workingMap.put(FileMapKeys.FILE_ID, fileId);

    if (configService.testInsertFault(ConfigEnum.FILE_INGEST_LOCK_CONFLICT_STOP_FAULT)) {
      try {
        logger.info("FILE_INGEST_LOCK_CONFLICT_STOP_FAULT");
        while (!configService.testInsertFault(
            ConfigEnum.FILE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT)) {
          logger.info("Sleeping for CONTINUE FAULT");
          TimeUnit.SECONDS.sleep(5);
        }
        logger.info("FILE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT");
      } catch (InterruptedException intEx) {
        Thread.currentThread().interrupt();
        throw new DatasetLockException("Unexpected interrupt during file ingest lock fault", intEx);
      }
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
