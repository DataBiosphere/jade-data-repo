package bio.terra.service.snapshotbuilder;

import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.model.*;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
    SnapshotBuilderProgramDataListOption listOption =
        (SnapshotBuilderProgramDataListOption)
            settings.getProgramDataOptions().stream()
                .filter(
                    programDataOption ->
                        Objects.equals(programDataOption.getId(), criteria.getId())
                            && programDataOption.getKind() == SnapshotBuilderOption.KindEnum.LIST)
                .findFirst()
                .orElseThrow(
                    () ->
                        new InternalServerErrorException(
                            String.format("No value found for criteria ID %d", criteria.getId())));
    return String.format(
        "The following concepts from %s: %s",
        listOption.getName(),
        criteria.getValues().stream()
            .map(
                criteriaValue ->
                    listOption.getValues().stream()
                        .filter(listValue -> Objects.equals(listValue.getId(), criteriaValue))
                        .findFirst()
                        .orElseThrow(
                            () ->
                                new InternalServerErrorException(
                                    String.format(
                                        "No value found for criteria concept ID %d",
                                        criteriaValue)))
                        .getName())
            .collect(Collectors.joining(", ")));
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
      SnapshotBuilderDomainCriteria criteria,
      SnapshotBuilderSettings settings,
      Map<Integer, String> conceptIdsToName) {
    return String.format(
        "%s Concept: %s",
        settings.getDomainOptions().stream()
            .filter(domainOption -> domainOption.getId().equals(criteria.getId()))
            .findFirst()
            .orElseThrow()
            .getName(),
        conceptIdsToName.get(criteria.getConceptId()));
  }

  @VisibleForTesting
  static String generateSummaryForCriteriaGroup(
      SnapshotBuilderCriteriaGroup criteriaGroup,
      SnapshotBuilderSettings settings,
      Map<Integer, String> conceptIdsToName) {
    return String.format(
        "Must %s %s:%n%s",
        criteriaGroup.isMustMeet() ? "meet" : "not meet",
        criteriaGroup.isMeetAll() ? "all of" : "any of",
        criteriaGroup.getCriteria().stream()
            .map(
                criteria ->
                    switch (criteria.getKind()) {
                      case LIST ->
                          generateSummaryForCriteria(
                              (SnapshotBuilderProgramDataListCriteria) criteria, settings);
                      case RANGE ->
                          generateSummaryForCriteria(
                              (SnapshotBuilderProgramDataRangeCriteria) criteria, settings);
                      case DOMAIN ->
                          generateSummaryForCriteria(
                              (SnapshotBuilderDomainCriteria) criteria, settings, conceptIdsToName);
                    })
            .collect(Collectors.joining("\n")));
  }

  @VisibleForTesting
  static String generateSummaryForCohort(
      SnapshotBuilderCohort cohort,
      SnapshotBuilderSettings settings,
      Map<Integer, String> conceptIdsToName) {
    return String.format(
        "Name: %s%nGroups:%n%s",
        cohort.getName(),
        cohort.getCriteriaGroups().stream()
            .map(
                criteriaGroup ->
                    generateSummaryForCriteriaGroup(criteriaGroup, settings, conceptIdsToName))
            .collect(Collectors.joining("\n")));
  }

  @VisibleForTesting
  String generateSummaryFromSnapshotSpecification(
      SnapshotBuilderSettings settings, Map<Integer, String> conceptIdsToName) {
    return snapshotSpecification != null
        ? String.format(
            "Participants included:%n%s%nTables included:%s%n",
            snapshotSpecification.getCohorts().stream()
                .map(cohort -> generateSummaryForCohort(cohort, settings, conceptIdsToName))
                .collect(Collectors.joining("\n")),
            snapshotSpecification.getOutputTables().stream()
                .map(SnapshotBuilderOutputTable::getName)
                .collect(Collectors.joining(", ")))
        : "No snapshot specification found";
  }

  @VisibleForTesting
  List<Integer> generateCriteriaGroupConceptIds(SnapshotBuilderCriteriaGroup criteriaGroup) {
    return criteriaGroup.getCriteria().stream()
        // Program data options include the string value in the settings,
        // so we don't need to fetch their names.
        .filter(criteria -> criteria.getKind() == SnapshotBuilderCriteria.KindEnum.DOMAIN)
        .map(criteria -> ((SnapshotBuilderDomainCriteria) criteria).getConceptId())
        .toList();
  }

  @VisibleForTesting
  List<Integer> generateCohortConceptIds(SnapshotBuilderCohort cohort) {
    return cohort.getCriteriaGroups().stream()
        .map(this::generateCriteriaGroupConceptIds)
        .flatMap(List::stream)
        .toList();
  }

  public List<Integer> generateConceptIds() {
    return snapshotSpecification != null
        ? snapshotSpecification.getCohorts().stream()
            .map(this::generateCohortConceptIds)
            .flatMap(List::stream)
            .distinct()
            .toList()
        : List.of();
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
        .createdSnapshotId(createdSnapshotId);
  }

  public SnapshotAccessRequestDetailsResponse generateModelDetails(
      SnapshotBuilderSettings settings, Map<Integer, String> conceptIdsToName) {
    return new SnapshotAccessRequestDetailsResponse()
        .summary(generateSummaryFromSnapshotSpecification(settings, conceptIdsToName));
  }
}
