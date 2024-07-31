package bio.terra.service.snapshotbuilder;

import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderOutputTable;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderRequest;
import bio.terra.model.SnapshotBuilderSettings;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public record SnapshotAccessRequestModel(
    UUID id,
    String snapshotName,
    String snapshotResearchPurpose,
    UUID sourceSnapshotId,
    SnapshotBuilderRequest snapshotSpecification,
    String createdBy,
    Instant createdDate,
    Instant statusUpdatedDate,
    SnapshotAccessRequestStatus status,
    UUID createdSnapshotId,
    String flightid) {

  @VisibleForTesting
  static String generateSummaryForCriteria(
      SnapshotBuilderProgramDataListCriteria criteria, SnapshotBuilderSettings settings) {
    return String.format(
        "The following concepts from %s: %s",
        settings.getProgramDataOptions().stream()
            .filter(
                programDataOption -> Objects.equals(programDataOption.getId(), criteria.getId()))
            .findFirst()
            .orElseThrow(
                () ->
                    new InternalServerErrorException(
                        String.format("No value found for criteria ID %d", criteria.getId())))
            .getName(),
        criteria.getValues().stream().map(Object::toString).collect(Collectors.joining(", ")));
  }

  @VisibleForTesting
  static String generateSummaryForCriteria(
      SnapshotBuilderProgramDataRangeCriteria criteria, SnapshotBuilderSettings settings) {
    return String.format(
        "%s between %d and %d",
        settings.getProgramDataOptions().stream()
            .filter(
                programDataOption -> Objects.equals(programDataOption.getId(), criteria.getId()))
            .findFirst()
            .orElseThrow()
            .getName(),
        criteria.getLow(),
        criteria.getHigh());
  }

  @VisibleForTesting
  static String generateSummaryForCriteria(
      SnapshotBuilderDomainCriteria criteria, SnapshotBuilderSettings settings) {
    return String.format(
        "%s Concept Id: %s",
        settings.getDomainOptions().stream()
            .filter(domainOption -> domainOption.getId().equals(criteria.getId()))
            .findFirst()
            .orElseThrow()
            .getName(),
        criteria.getConceptId());
  }

  @VisibleForTesting
  static String generateSummaryForCriteriaGroup(
      SnapshotBuilderCriteriaGroup criteriaGroup, SnapshotBuilderSettings settings) {
    return String.format(
        "Must %s %s:\n%s",
        criteriaGroup.isMustMeet() ? "meet" : "not meet",
        criteriaGroup.isMeetAll() ? "all of" : "any of",
        criteriaGroup.getCriteria().stream()
            .map(
                criteria ->
                    switch (criteria.getKind()) {
                      case LIST -> generateSummaryForCriteria(
                          (SnapshotBuilderProgramDataListCriteria) criteria, settings);
                      case RANGE -> generateSummaryForCriteria(
                          (SnapshotBuilderProgramDataRangeCriteria) criteria, settings);
                      case DOMAIN -> generateSummaryForCriteria(
                          (SnapshotBuilderDomainCriteria) criteria, settings);
                    })
            .collect(Collectors.joining("\n")));
  }

  @VisibleForTesting
  static String generateSummaryForCohort(
      SnapshotBuilderCohort cohort, SnapshotBuilderSettings settings) {
    return String.format(
        "Name: %s\nGroups:\n%s",
        cohort.getName(),
        cohort.getCriteriaGroups().stream()
            .map(criteriaGroup -> generateSummaryForCriteriaGroup(criteriaGroup, settings))
            .collect(Collectors.joining("\n")));
  }

  @VisibleForTesting
  String generateSummaryFromSnapshotSpecification(SnapshotBuilderSettings settings) {
    return String.format(
        "Participants included:\n%s\nTables included:%s\n",
        snapshotSpecification.getCohorts().stream()
            .map(cohort -> generateSummaryForCohort(cohort, settings))
            .collect(Collectors.joining("\n")),
        snapshotSpecification.getOutputTables().stream()
            .map(SnapshotBuilderOutputTable::getName)
            .collect(Collectors.joining(", ")));
  }

  public SnapshotAccessRequestResponse toApiResponse(SnapshotBuilderSettings settings) {
    return new SnapshotAccessRequestResponse()
        .id(id)
        .flightid(flightid)
        .sourceSnapshotId(sourceSnapshotId)
        .snapshotName(snapshotName)
        .snapshotResearchPurpose(snapshotResearchPurpose)
        .snapshotSpecification(snapshotSpecification)
        .createdBy(createdBy)
        .status(status)
        .createdDate(createdDate != null ? createdDate.toString() : null)
        .statusUpdatedDate(statusUpdatedDate != null ? statusUpdatedDate.toString() : null)
        .createdSnapshotId(createdSnapshotId)
        .summary(generateSummaryFromSnapshotSpecification(settings));
  }
}
