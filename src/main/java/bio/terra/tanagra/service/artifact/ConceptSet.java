package bio.terra.tanagra.service.artifact;

import bio.terra.tanagra.exception.SystemException;
import java.time.OffsetDateTime;
import javax.annotation.Nullable;

public class ConceptSet {
  private final String studyId;
  private final String conceptSetId;
  private final String underlayName;
  private final String entityName;
  private final OffsetDateTime created;
  private final String createdBy;
  private final OffsetDateTime lastModified;
  private final @Nullable String displayName;
  private final @Nullable String description;
  private final Criteria criteria;

  private ConceptSet(Builder builder) {
    this.studyId = builder.studyId;
    this.conceptSetId = builder.conceptSetId;
    this.underlayName = builder.underlayName;
    this.entityName = builder.entityName;
    this.created = builder.created;
    this.createdBy = builder.createdBy;
    this.lastModified = builder.lastModified;
    this.displayName = builder.displayName;
    this.description = builder.description;
    this.criteria = builder.criteria;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return builder()
        .studyId(studyId)
        .conceptSetId(conceptSetId)
        .underlayName(underlayName)
        .entityName(entityName)
        .created(created)
        .createdBy(createdBy)
        .lastModified(lastModified)
        .displayName(displayName)
        .description(description)
        .criteria(criteria);
  }

  /** Globally unique identifier of the study this concept set belongs to. */
  public String getStudyId() {
    return studyId;
  }

  /** Unique (per study) identifier of this concept set. */
  public String getConceptSetId() {
    return conceptSetId;
  }

  /** Globally unique name of the underlay this concept set is pinned to. */
  public String getUnderlayName() {
    return underlayName;
  }

  /** Name of the entity this concept set is pinned to. */
  public String getEntityName() {
    return entityName;
  }

  /** Timestamp of when this concept set was created. */
  public OffsetDateTime getCreated() {
    return created;
  }

  /** Email of user who created this concept set. */
  public String getCreatedBy() {
    return createdBy;
  }

  /** Timestamp of when this concept set was last modified. */
  public OffsetDateTime getLastModified() {
    return lastModified;
  }

  /** Optional display name for the concept set. */
  public String getDisplayName() {
    return displayName;
  }

  /** Optional description for the concept set. */
  public String getDescription() {
    return description;
  }

  /** Criteria that defines the entity filter. */
  public Criteria getCriteria() {
    return criteria;
  }

  public static class Builder {
    private String studyId;
    private String conceptSetId;
    private String underlayName;
    private String entityName;
    private OffsetDateTime created;
    private String createdBy;
    private OffsetDateTime lastModified;
    private @Nullable String displayName;
    private @Nullable String description;
    private Criteria criteria;

    public Builder studyId(String studyId) {
      this.studyId = studyId;
      return this;
    }

    public Builder conceptSetId(String conceptSetId) {
      this.conceptSetId = conceptSetId;
      return this;
    }

    public Builder underlayName(String underlayName) {
      this.underlayName = underlayName;
      return this;
    }

    public Builder entityName(String entityName) {
      this.entityName = entityName;
      return this;
    }

    public Builder created(OffsetDateTime created) {
      this.created = created;
      return this;
    }

    public Builder createdBy(String createdBy) {
      this.createdBy = createdBy;
      return this;
    }

    public Builder lastModified(OffsetDateTime lastModified) {
      this.lastModified = lastModified;
      return this;
    }

    public Builder displayName(String displayName) {
      this.displayName = displayName;
      return this;
    }

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder criteria(Criteria criteria) {
      this.criteria = criteria;
      return this;
    }

    public String getConceptSetId() {
      return conceptSetId;
    }

    public ConceptSet build() {
      if (conceptSetId == null) {
        throw new SystemException("Concept set requires id");
      }
      return new ConceptSet(this);
    }
  }
}
