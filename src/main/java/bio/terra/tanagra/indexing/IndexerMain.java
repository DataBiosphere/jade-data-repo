package bio.terra.tanagra.indexing;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.jobexecutor.JobRunner;
import bio.terra.tanagra.query.azure.AzureExecutor;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.datapointer.AzureDataset;
import bio.terra.tanagra.utils.FileIO;
import bio.terra.tanagra.utils.FileUtils;
import java.nio.file.Path;
import org.springframework.stereotype.Component;

@Component
public final class IndexerMain {

  enum Command {
    EXPAND_CONFIG,
    INDEX_ENTITY,
    INDEX_ENTITY_GROUP,
    INDEX_ALL,
    CLEAN_ENTITY,
    CLEAN_ENTITY_GROUP,
    CLEAN_ALL
  }

  private final AzureExecutor azureExecutor;

  public IndexerMain(AzureExecutor azureExecutor) {
    this.azureExecutor = azureExecutor;
  }

  /** IndexerMain entrypoint for running indexing. */
  public void run(String... args) throws Exception {
    // TODO: Consider using the picocli library for command parsing and packaging this as an actual
    // CLI.
    Command cmd = Command.valueOf(args[0]);
    //    String underlayFilePath = args[1];
    String underlayFilePath =
        "/Users/pshapiro/source/jade-data-repo/src/main/resources/tanagra/cms_synpuf/original/cms_synpuf.json";

    // TODO: Use singleton FileIO instance instead of setting a bunch of separate static properties.
    FileIO.setToReadDiskFiles(); // This is the default, included here for clarity.
    FileIO.setInputParentDir(Path.of(underlayFilePath).toAbsolutePath().getParent());
    Indexer indexer =
        Indexer.deserializeUnderlay(Path.of(underlayFilePath).getFileName().toString());

    for (DataPointer dataPointer : indexer.getUnderlay().getDataPointers().values()) {
      if (dataPointer instanceof AzureDataset azureDataset) {
        azureExecutor.setupAccess(azureDataset);
      }
    }

    switch (cmd) {
      case EXPAND_CONFIG:
        String outputDirPath = args[2];

        FileIO.setOutputParentDir(Path.of(outputDirPath));
        FileUtils.createDirectoryIfNonexistent(FileIO.getOutputParentDir());

        indexer.scanSourceData(azureExecutor);

        indexer.maybeAddAgeAtOccurrenceAttribute();

        indexer.getUnderlay().serializeAndWriteToFile();
        break;
      case INDEX_ENTITY:
      case CLEAN_ENTITY:
        IndexingJob.RunType runTypeEntity =
            Command.INDEX_ENTITY == cmd ? IndexingJob.RunType.RUN : IndexingJob.RunType.CLEAN;
        String nameEntity = args[2];
        boolean isAllEntities = "*".equals(nameEntity);
        boolean isDryRunEntity = isDryRun(3, args);
        Indexer.JobExecutor jobExecEntity = getJobExec(4, args);

        // Index/clean all the entities (*) or just one (entityName).
        JobRunner entityJobRunner;
        if (isAllEntities) {
          entityJobRunner =
              indexer.runJobsForAllEntities(
                  jobExecEntity, isDryRunEntity, runTypeEntity, azureExecutor);
        } else {
          entityJobRunner =
              indexer.runJobsForSingleEntity(
                  jobExecEntity, isDryRunEntity, runTypeEntity, nameEntity, azureExecutor);
        }
        entityJobRunner.printJobResultSummary();
        entityJobRunner.throwIfAnyFailures();
        break;
      case INDEX_ENTITY_GROUP:
      case CLEAN_ENTITY_GROUP:
        IndexingJob.RunType runTypeEntityGroup =
            Command.INDEX_ENTITY_GROUP == cmd ? IndexingJob.RunType.RUN : IndexingJob.RunType.CLEAN;
        String nameEntityGroup = args[2];
        boolean isAllEntityGroups = "*".equals(nameEntityGroup);
        boolean isDryRunEntityGroup = isDryRun(3, args);
        Indexer.JobExecutor jobExecEntityGroup = getJobExec(4, args);

        // Index/clean all the entity groups (*) or just one (entityGroupName).
        JobRunner entityGroupJobRunner;
        if (isAllEntityGroups) {
          entityGroupJobRunner =
              indexer.runJobsForAllEntityGroups(
                  jobExecEntityGroup, isDryRunEntityGroup, runTypeEntityGroup, azureExecutor);
        } else {
          entityGroupJobRunner =
              indexer.runJobsForSingleEntityGroup(
                  jobExecEntityGroup,
                  isDryRunEntityGroup,
                  runTypeEntityGroup,
                  nameEntityGroup,
                  azureExecutor);
        }
        entityGroupJobRunner.printJobResultSummary();
        entityGroupJobRunner.throwIfAnyFailures();
        break;
      case INDEX_ALL:
      case CLEAN_ALL:
        IndexingJob.RunType runTypeAll =
            Command.INDEX_ALL == cmd ? IndexingJob.RunType.RUN : IndexingJob.RunType.CLEAN;
        boolean isDryRunAll = isDryRun(2, args);
        Indexer.JobExecutor jobExecAll = getJobExec(3, args);

        // Index/clean all the entities and entity groups.
        JobRunner entityJobRunnerAll =
            indexer.runJobsForAllEntities(jobExecAll, isDryRunAll, runTypeAll, azureExecutor);
        JobRunner entityGroupJobRunnerAll =
            indexer.runJobsForAllEntityGroups(jobExecAll, isDryRunAll, runTypeAll, azureExecutor);
        entityJobRunnerAll.printJobResultSummary();
        entityGroupJobRunnerAll.printJobResultSummary();
        entityJobRunnerAll.throwIfAnyFailures();
        entityGroupJobRunnerAll.throwIfAnyFailures();
        break;
      default:
        throw new SystemException("Unknown command: " + cmd);
    }
  }

  private static boolean isDryRun(int index, String... args) {
    return true;
//    return args.length > index && "DRY_RUN".equals(args[index]);
  }

  private static Indexer.JobExecutor getJobExec(int index, String... args) {
    return args.length > index && "SERIAL".equals(args[index])
        ? Indexer.JobExecutor.SERIAL
        : Indexer.JobExecutor.PARALLEL;
  }
}
