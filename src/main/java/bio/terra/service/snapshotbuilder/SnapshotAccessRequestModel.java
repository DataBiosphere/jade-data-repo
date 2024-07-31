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

public class SnapshotAccessRequestModel {
  private UUID id;
  private String snapshotName;
  private String snapshotResearchPurpose;
  private UUID sourceSnapshotId;
  private SnapshotBuilderRequest snapshotSpecification;
  private String createdBy;
  private Instant createdDate;
  private Instant statusUpdatedDate;
  private SnapshotAccessRequestStatus status;
  private UUID createdSnapshotId;
  private String flightid;

  public SnapshotAccessRequestModel() {}

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public SnapshotAccessRequestModel id(UUID id) {
    this.id = id;
    return this;
  }

  public String getSnapshotName() {
    return snapshotName;
  }

  public void setSnapshotName(String snapshotName) {
    this.snapshotName = snapshotName;
  }

  public SnapshotAccessRequestModel snapshotName(String snapshotName) {
    this.snapshotName = snapshotName;
    return this;
  }

  public String getSnapshotResearchPurpose() {
    return snapshotResearchPurpose;
  }

  public void setSnapshotResearchPurpose(String snapshotResearchPurpose) {
    this.snapshotResearchPurpose = snapshotResearchPurpose;
  }

  public SnapshotAccessRequestModel snapshotResearchPurpose(String snapshotResearchPurpose) {
    this.snapshotResearchPurpose = snapshotResearchPurpose;
    return this;
  }

  public UUID getSourceSnapshotId() {
    return sourceSnapshotId;
  }

  public void setSourceSnapshotId(UUID sourceSnapshotId) {
    this.sourceSnapshotId = sourceSnapshotId;
  }

  public SnapshotAccessRequestModel sourceSnapshotId(UUID sourceSnapshotId) {
    this.sourceSnapshotId = sourceSnapshotId;
    return this;
  }

  public SnapshotBuilderRequest getSnapshotSpecification() {
    return snapshotSpecification;
  }

  public void setSnapshotSpecification(SnapshotBuilderRequest snapshotSpecification) {
    this.snapshotSpecification = snapshotSpecification;
  }

  public SnapshotAccessRequestModel snapshotSpecification(
      SnapshotBuilderRequest snapshotSpecification) {
    this.snapshotSpecification = snapshotSpecification;
    return this;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public void setCreatedBy(String createdBy) {
    this.createdBy = createdBy;
  }

  public SnapshotAccessRequestModel createdBy(String createdBy) {
    this.createdBy = createdBy;
    return this;
  }

  public Instant getCreatedDate() {
    return createdDate;
  }

  public void setCreatedDate(Instant createdDate) {
    this.createdDate = createdDate;
  }

  public SnapshotAccessRequestModel createdDate(Instant createdDate) {
    this.createdDate = createdDate;
    return this;
  }

  public Instant getStatusUpdatedDate() {
    return statusUpdatedDate;
  }

  public void setStatusUpdatedDate(Instant statusUpdatedDate) {
    this.statusUpdatedDate = statusUpdatedDate;
  }

  public SnapshotAccessRequestModel statusUpdatedDate(Instant statusUpdatedDate) {
    this.statusUpdatedDate = statusUpdatedDate;
    return this;
  }

  public SnapshotAccessRequestStatus getStatus() {
    return status;
  }

  public void setStatus(SnapshotAccessRequestStatus status) {
    this.status = status;
  }

  public SnapshotAccessRequestModel status(SnapshotAccessRequestStatus status) {
    this.status = status;
    return this;
  }

  public UUID getCreatedSnapshotId() {
    return createdSnapshotId;
  }

  public void setCreatedSnapshotId(UUID createdSnapshotId) {
    this.createdSnapshotId = createdSnapshotId;
  }

  public SnapshotAccessRequestModel createdSnapshotId(UUID createdSnapshotId) {
    this.createdSnapshotId = createdSnapshotId;
    return this;
  }

  public String getFlightid() {
    return flightid;
  }

  public void setFlightid(String flightid) {
    this.flightid = flightid;
  }

  public SnapshotAccessRequestModel flightid(String flightid) {
    this.flightid = flightid;
    return this;
  }

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
            .map(cohort -> this.generateSummaryForCohort(cohort, settings))
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
