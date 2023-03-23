package bio.terra.tanagra.indexing;

import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.AGE_AT_OCCURRENCE_ATTRIBUTE_NAME;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.job.BuildNumChildrenAndPaths;
import bio.terra.tanagra.indexing.job.BuildTextSearchStrings;
import bio.terra.tanagra.indexing.job.ComputeAgeAtOccurrence;
import bio.terra.tanagra.indexing.job.ComputeDisplayHints;
import bio.terra.tanagra.indexing.job.ComputeRollupCounts;
import bio.terra.tanagra.indexing.job.CreateEntityTable;
import bio.terra.tanagra.indexing.job.DenormalizeEntityInstances;
import bio.terra.tanagra.indexing.job.WriteAncestorDescendantIdPairs;
import bio.terra.tanagra.indexing.job.WriteParentChildIdPairs;
import bio.terra.tanagra.indexing.job.WriteRelationshipIdPairs;
import bio.terra.tanagra.indexing.jobexecutor.JobRunner;
import bio.terra.tanagra.indexing.jobexecutor.ParallelRunner;
import bio.terra.tanagra.indexing.jobexecutor.SequencedJobSet;
import bio.terra.tanagra.indexing.jobexecutor.SerialRunner;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.Literal.DataType;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.azure.AzureExecutor;
import bio.terra.tanagra.underlay.*;
import bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Indexer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Indexer.class);

  public enum JobExecutor {
    PARALLEL,
    SERIAL;

    public JobRunner getRunner(
        List<SequencedJobSet> jobSets,
        boolean isDryRun,
        IndexingJob.RunType runType,
        QueryExecutor executor) {
      return switch (this) {
        case SERIAL -> new SerialRunner(jobSets, isDryRun, runType, executor);
        case PARALLEL -> new ParallelRunner(jobSets, isDryRun, runType, executor);
      };
    }
  }

  private final Underlay underlay;

  private Indexer(Underlay underlay) {
    this.underlay = underlay;
  }

  /** Deserialize the POJOs to the internal objects and expand all defaults. */
  public static Indexer deserializeUnderlay(String underlayFileName) throws IOException {
    return new Indexer(Underlay.fromJSON(underlayFileName));
  }

  /** Scan the source data to validate data pointers, lookup data types, generate UI hints, etc. */
  public void scanSourceData(AzureExecutor executor) {
    // TODO: Validate existence and access for data/table/field pointers.
    underlay
        .getEntities()
        .values()
        .forEach(
            e -> {
              LOGGER.info(
                  "Looking up attribute data types and generating UI hints for entity: "
                      + e.getName());
              e.scanSourceData(executor);
            });
  }

  /**
   * For CRITERIA_OCCURRENCE entity group occurrence entities (eg condition_occurrence), if
   * occurrence entity config has sourceStartDateColumn, add age_at_occurrernce attribute to
   * occurrence entity.
   */
  public void maybeAddAgeAtOccurrenceAttribute() {
    underlay
        .getEntityGroups()
        .values()
        .forEach(
            entityGroup -> {
              switch (entityGroup.getType()) {
                case GROUP_ITEMS:
                  return;
                case CRITERIA_OCCURRENCE:
                  CriteriaOccurrence criteriaOccurrence = (CriteriaOccurrence) entityGroup;
                  if (criteriaOccurrence.getOccurrenceEntity().getSourceStartDateColumn() == null) {
                    return;
                  }
                  // TODO: Confirm sourceStartDateColumn is DATE, DATETIME or TIMESTAMP
                  if (underlay.getPrimaryEntity().getSourceStartDateColumn() == null) {
                    throw new InvalidConfigException(
                        String.format(
                            "Occurrence entity %s config has sourceStartDateColumn. Primary entity config must have sourceStartDateColumn. Please set sourceStartDateColumn in primary entity config.",
                            criteriaOccurrence.getName()));
                  }
                  LOGGER.info("Adding age_at_occurrence attribute to {}", entityGroup.getName());
                  Attribute attribute =
                      new Attribute(
                          AGE_AT_OCCURRENCE_ATTRIBUTE_NAME,
                          Attribute.Type.SIMPLE,
                          DataType.INT64,
                          /*displayHint=*/ null);
                  FieldPointer fieldPointer =
                      new FieldPointer.Builder()
                          .tablePointer(
                              criteriaOccurrence
                                  .getOccurrenceEntity()
                                  .getMapping(Underlay.MappingType.INDEX)
                                  .getTablePointer())
                          .columnName(AGE_AT_OCCURRENCE_ATTRIBUTE_NAME)
                          .build();
                  attribute.initialize(/*sourceMapping=*/ null, new AttributeMapping(fieldPointer));
                  criteriaOccurrence
                      .getOccurrenceEntity()
                      .addAttribute(AGE_AT_OCCURRENCE_ATTRIBUTE_NAME, attribute);
                  return;
                default:
                  throw new SystemException("Unknown entity group type: " + entityGroup.getType());
              }
            });
  }

  public JobRunner runJobsForAllEntities(
      JobExecutor jobExecutor,
      boolean isDryRun,
      IndexingJob.RunType runType,
      QueryExecutor azureExecutor) {
    LOGGER.info("INDEXING all entities");
    List<SequencedJobSet> jobSets =
        underlay.getEntities().values().stream()
            .map(this::getJobSetForEntity)
            .collect(Collectors.toList());
    return runJobs(jobExecutor, isDryRun, runType, jobSets, azureExecutor);
  }

  public JobRunner runJobsForSingleEntity(
      JobExecutor jobExecutor,
      boolean isDryRun,
      IndexingJob.RunType runType,
      String name,
      AzureExecutor azureExecutor) {
    LOGGER.info("INDEXING entity: {}", name);
    List<SequencedJobSet> jobSets = List.of(getJobSetForEntity(underlay.getEntity(name)));
    return runJobs(jobExecutor, isDryRun, runType, jobSets, azureExecutor);
  }

  public JobRunner runJobsForAllEntityGroups(
      JobExecutor jobExecutor,
      boolean isDryRun,
      IndexingJob.RunType runType,
      AzureExecutor azureExecutor) {
    LOGGER.info("INDEXING all entity groups");
    List<SequencedJobSet> jobSets =
        underlay.getEntityGroups().values().stream()
            .map(Indexer::getJobSetForEntityGroup)
            .collect(Collectors.toList());
    return runJobs(jobExecutor, isDryRun, runType, jobSets, azureExecutor);
  }

  public JobRunner runJobsForSingleEntityGroup(
      JobExecutor jobExecutor,
      boolean isDryRun,
      IndexingJob.RunType runType,
      String name,
      AzureExecutor azureExecutor) {
    LOGGER.info("INDEXING entity group: {}", name);
    List<SequencedJobSet> jobSets = List.of(getJobSetForEntityGroup(underlay.getEntityGroup(name)));
    return runJobs(jobExecutor, isDryRun, runType, jobSets, azureExecutor);
  }

  private JobRunner runJobs(
      JobExecutor jobExecutor,
      boolean isDryRun,
      IndexingJob.RunType runType,
      List<SequencedJobSet> jobSets,
      QueryExecutor azureExecutor) {
    JobRunner jobRunner = jobExecutor.getRunner(jobSets, isDryRun, runType, azureExecutor);
    jobRunner.runJobSets();
    return jobRunner;
  }

  @VisibleForTesting
  public SequencedJobSet getJobSetForEntity(Entity entity) {
    SequencedJobSet jobSet = new SequencedJobSet(entity.getName());
    jobSet.startNewStage();
    jobSet.addJob(new CreateEntityTable(entity));

    jobSet.startNewStage();
    jobSet.addJob(new DenormalizeEntityInstances(entity));

    if (entity.getTextSearch().isEnabled() || entity.hasHierarchies()) {
      jobSet.startNewStage();
    }

    if (entity.getTextSearch().isEnabled()) {
      jobSet.addJob(new BuildTextSearchStrings(entity));
    }
    entity.getHierarchies().stream()
        .forEach(
            hierarchy -> {
              jobSet.addJob(new WriteParentChildIdPairs(entity, hierarchy.getName()));
              jobSet.addJob(new WriteAncestorDescendantIdPairs(entity, hierarchy.getName()));
              jobSet.addJob(new BuildNumChildrenAndPaths(entity, hierarchy.getName()));
            });

    if (entity.getAttribute(AGE_AT_OCCURRENCE_ATTRIBUTE_NAME) != null) {
      Relationship occurrencePrimaryRelationship =
          ((CriteriaOccurrence)
                  underlay.getEntityGroup(EntityGroup.Type.CRITERIA_OCCURRENCE, entity))
              .getOccurrencePrimaryRelationship();
      jobSet.addJob(new ComputeAgeAtOccurrence(entity, occurrencePrimaryRelationship));
    }

    return jobSet;
  }

  @VisibleForTesting
  public static SequencedJobSet getJobSetForEntityGroup(EntityGroup entityGroup) {
    SequencedJobSet jobSet = new SequencedJobSet(entityGroup.getName());
    jobSet.startNewStage();

    // For each relationship, write the index relationship mapping.
    entityGroup.getRelationships().values().stream()
        .forEach(
            // TODO: If the source relationship mapping table = one of the entity tables, then just
            // populate a new column on that entity table, instead of always writing a new table.
            relationship -> jobSet.addJob(new WriteRelationshipIdPairs(relationship)));

    if (EntityGroup.Type.CRITERIA_OCCURRENCE.equals(entityGroup.getType())) {
      CriteriaOccurrence criteriaOccurrence = (CriteriaOccurrence) entityGroup;
      // Compute the criteria rollup counts for both the criteria-primary and criteria-occurrence
      // relationships.
      jobSet.addJob(
          new ComputeRollupCounts(
              criteriaOccurrence.getCriteriaEntity(),
              criteriaOccurrence.getCriteriaPrimaryRelationship(),
              null));
      jobSet.addJob(
          new ComputeRollupCounts(
              criteriaOccurrence.getCriteriaEntity(),
              criteriaOccurrence.getOccurrenceCriteriaRelationship(),
              null));

      // If the criteria entity has a hierarchy, then also compute the counts for each
      // hierarchy.
      if (criteriaOccurrence.getCriteriaEntity().hasHierarchies()) {
        criteriaOccurrence.getCriteriaEntity().getHierarchies().stream()
            .forEach(
                hierarchy -> {
                  jobSet.addJob(
                      new ComputeRollupCounts(
                          criteriaOccurrence.getCriteriaEntity(),
                          criteriaOccurrence.getCriteriaPrimaryRelationship(),
                          hierarchy));
                  jobSet.addJob(
                      new ComputeRollupCounts(
                          criteriaOccurrence.getCriteriaEntity(),
                          criteriaOccurrence.getOccurrenceCriteriaRelationship(),
                          hierarchy));
                });
      }

      // Compute display hints for the occurrence entity.
      if (!criteriaOccurrence.getModifierAttributes().isEmpty()) {
        jobSet.addJob(
            new ComputeDisplayHints(
                criteriaOccurrence, criteriaOccurrence.getModifierAttributes()));
      }
    }
    return jobSet;
  }

  public Underlay getUnderlay() {
    return underlay;
  }
}
