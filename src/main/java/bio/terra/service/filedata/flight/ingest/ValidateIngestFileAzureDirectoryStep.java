package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.FlightUtils;
import bio.terra.model.FileLoadModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.exception.FileAlreadyExistsException;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.function.Predicate;

public class ValidateIngestFileAzureDirectoryStep extends SkippableStep {
  public static final String CREATE_ENTRY_ACTION = "createEntry";
  public static final String CHECK_ENTRY_ACTION = "checkEntry";

  private final TableDao tableDao;
  private final Dataset dataset;

  public ValidateIngestFileAzureDirectoryStep(
      TableDao tableDao, Dataset dataset, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.tableDao = tableDao;
    this.dataset = dataset;
  }

  public ValidateIngestFileAzureDirectoryStep(TableDao tableDao, Dataset dataset) {
    this(tableDao, dataset, SkippableStep::neverSkip);
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) throws InterruptedException {
    FlightMap inputParameters = context.getInputParameters();
    FileLoadModel loadModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

    String targetPath = loadModel.getTargetPath();
    FlightMap workingMap = context.getWorkingMap();

    try {
      //  1. If the directory entry does not exist, update INGEST_FILE_ACTION to createEntry
      //  2. If the directory entry exists:
      //      (a) If the loadTags do not match, then we throw FileAlreadyExistsException.
      //      (b) Otherwise, update INGEST_FILE_ACTION to checkEntry
      AzureStorageAuthInfo storageAuthInfo =
          FlightUtils.getContextValue(
              context, CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
      FireStoreDirectoryEntry existingEntry =
          tableDao.lookupDirectoryEntryByPath(dataset, targetPath, storageAuthInfo);
      if (existingEntry == null) {
        workingMap.put(FileMapKeys.INGEST_FILE_ACTION, CREATE_ENTRY_ACTION);
      } else if (!existingEntry.getLoadTag().equals(loadModel.getLoadTag())) {
        throw new FileAlreadyExistsException("Path already exists: " + targetPath);
      } else {
        workingMap.put(FileMapKeys.INGEST_FILE_ACTION, CHECK_ENTRY_ACTION);
      }
    } catch (FileSystemAbortTransactionException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }

    return StepResult.getStepResultSuccess();
  }
}
