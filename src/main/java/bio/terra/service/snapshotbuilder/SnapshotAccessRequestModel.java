package bio.terra.service.snapshotbuilder;

import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.model.SnapshotAccessRequestDetailsResponse;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderDomainCriteria;
import bio.terra.model.SnapshotBuilderOption;
import bio.terra.model.SnapshotBuilderOutputTable;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListOption;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.model.SnapshotBuilderRequest;
import bio.terra.model.SnapshotBuilderSettings;
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
    String flightid,
    String samGroupName,
    String samGroupCreatedByTerraId) {

  @VisibleForTesting
  public record SummaryGenerator(
      SnapshotBuilderSettings settings, Map<Integer, String> conceptIdsToName) {
    String generateSummaryForCriteria(SnapshotBuilderProgramDataRangeCriteria criteria) {
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

    String generateSummaryForCriteria(SnapshotBuilderDomainCriteria criteria) {
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
    String generateSummaryForCriteria(SnapshotBuilderProgramDataListCriteria criteria) {
      SnapshotBuilderProgramDataListOption listOption =
          settings.getProgramDataOptions().stream()
              .filter(
                  programDataOption ->
                      programDataOption.getId().equals(criteria.getId())
                          && programDataOption.getKind() == SnapshotBuilderOption.KindEnum.LIST)
              .map(SnapshotBuilderProgramDataListOption.class::cast)
              .findFirst()
              .orElseThrow(
                  () ->
                      new InternalServerErrorException(
                          "No value found for criteria ID %d".formatted(criteria.getId())));
      return "The following concepts from %s: %s"
          .formatted(
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
                                          "No value found for criteria concept ID %d"
                                              .formatted(criteriaValue)))
                              .getName())
                  .collect(Collectors.joining(", ")));
    }

    String generateSummaryForCriteriaGroup(SnapshotBuilderCriteriaGroup criteriaGroup) {
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
                                (SnapshotBuilderProgramDataListCriteria) criteria);
                        case RANGE ->
                            generateSummaryForCriteria(
                                (SnapshotBuilderProgramDataRangeCriteria) criteria);
                        case DOMAIN ->
                            generateSummaryForCriteria((SnapshotBuilderDomainCriteria) criteria);
                      })
              .collect(Collectors.joining("\n")));
    }

    @VisibleForTesting
    String generateSummaryForCohort(SnapshotBuilderCohort cohort) {
      return String.format(
          "Name: %s%nGroups:%n%s",
          cohort.getName(),
          cohort.getCriteriaGroups().stream()
              .map(this::generateSummaryForCriteriaGroup)
              .collect(Collectors.joining("\n")));
    }

    @VisibleForTesting
    String generateSummaryFromSnapshotSpecification(SnapshotBuilderRequest snapshotSpecification) {
      return snapshotSpecification != null
          ? String.format(
              "Participants included:%n%s%nTables included:%s%n",
              snapshotSpecification.getCohorts().stream()
                  .map(this::generateSummaryForCohort)
                  .collect(Collectors.joining("\n")),
              snapshotSpecification.getOutputTables().stream()
                  .map(SnapshotBuilderOutputTable::getName)
                  .collect(Collectors.joining(", ")))
          : "No snapshot specification found";
    }
  }

  public List<Integer> generateConceptIds() {
    return snapshotSpecification != null
        ? snapshotSpecification.getCohorts().stream()
            .flatMap(cohort -> cohort.getCriteriaGroups().stream())
            .flatMap(criteriaGroup -> criteriaGroup.getCriteria().stream())
            .filter(criteria -> criteria.getKind() == SnapshotBuilderCriteria.KindEnum.DOMAIN)
            .map(criteria -> ((SnapshotBuilderDomainCriteria) criteria).getConceptId())
            .distinct()
            .toList()
        : List.of();
  }

  public SnapshotAccessRequestResponse toApiResponse() {
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
        .authGroupName(samGroupName)
        .createdSnapshotId(createdSnapshotId);
  }

  public SnapshotAccessRequestDetailsResponse generateModelDetails(
      SnapshotBuilderSettings settings, Map<Integer, String> conceptIdsToName) {
    return new SnapshotAccessRequestDetailsResponse()
        .summary(
            new SnapshotAccessRequestModel.SummaryGenerator(settings, conceptIdsToName)
                .generateSummaryFromSnapshotSpecification(snapshotSpecification));
  }
}
