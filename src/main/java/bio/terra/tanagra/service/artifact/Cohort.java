package bio.terra.tanagra.service.artifact;

import bio.terra.tanagra.exception.SystemException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class Cohort {
  public static final int STARTING_VERSION = 1;

  private final String studyId;
  private final String cohortId;
  private final String underlayName;
  private final String cohortRevisionGroupId;
  private final int version;
  private final boolean isMostRecent;
  private final boolean isEditable;
  private final OffsetDateTime created;
  private final String createdBy;
  private final OffsetDateTime lastModified;
  private final @Nullable String displayName;
  private final @Nullable String description;
  private final List<CriteriaGroup> criteriaGroups;

  private Cohort(Builder builder) {
    this.studyId = builder.studyId;
    this.cohortId = builder.cohortId;
    this.underlayName = builder.underlayName;
    this.cohortRevisionGroupId = builder.cohortRevisionGroupId;
    this.version = builder.version;
    this.isMostRecent = builder.isMostRecent;
    this.isEditable = builder.isEditable;
    this.created = builder.created;
    this.createdBy = builder.createdBy;
    this.lastModified = builder.lastModified;
    this.displayName = builder.displayName;
    this.description = builder.description;
    this.criteriaGroups = builder.criteriaGroups;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return builder()
        .studyId(studyId)
        .cohortId(cohortId)
        .underlayName(underlayName)
        .cohortRevisionGroupId(cohortRevisionGroupId)
        .version(version)
        .isMostRecent(isMostRecent)
        .isEditable(isEditable)
        .created(created)
        .createdBy(createdBy)
        .lastModified(lastModified)
        .displayName(displayName)
        .description(description)
        .criteriaGroups(criteriaGroups);
  }

  /** Globally unique identifier of the study this cohort belongs to. */
  public String getStudyId() {
    return studyId;
  }

  /** Unique (per study) identifier of this cohort + version. */
  public String getCohortId() {
    return cohortId;
  }

  /** Globally unique name of the underlay this cohort is pinned to. */
  public String getUnderlayName() {
    return underlayName;
  }

  /**
   * User-facing unique (per study) identifier of this set of cohort revisions (current and all
   * frozen ones).
   */
  public String getCohortRevisionGroupId() {
    return cohortRevisionGroupId;
  }

  /** Integer version of the cohort. */
  public int getVersion() {
    return version;
  }

  /** True if this cohort is the most recent. */
  public boolean isMostRecent() {
    return isMostRecent;
  }

  /**
   * True if this cohort is editable. False if the cohort has been frozen (e.g. by someone creating
   * a review).
   */
  public boolean isEditable() {
    return isEditable;
  }

  /** Timestamp of when this cohort was created. */
  public OffsetDateTime getCreated() {
    return created;
  }

  /** Email of user who created this cohort. */
  public String getCreatedBy() {
    return createdBy;
  }

  /** Timestamp of when this cohort was last modified. */
  public OffsetDateTime getLastModified() {
    return lastModified;
  }

  /** Optional display name for the cohort. */
  public String getDisplayName() {
    return displayName;
  }

  /** Optional description for the cohort. */
  public String getDescription() {
    return description;
  }

  /** List of criteria groups in the cohort. */
  public List<CriteriaGroup> getCriteriaGroups() {
    return Collections.unmodifiableList(criteriaGroups);
  }

  public void addCriteriaGroup(CriteriaGroup criteriaGroup) {
    criteriaGroups.add(criteriaGroup);
  }

  public static class Builder {
    private String studyId;
    private String cohortId;
    private String underlayName;
    private String cohortRevisionGroupId;
    private int version;
    private boolean isMostRecent;
    private boolean isEditable;
    private OffsetDateTime created;
    private String createdBy;
    private OffsetDateTime lastModified;
    private @Nullable String displayName;
    private @Nullable String description;
    private List<CriteriaGroup> criteriaGroups = new ArrayList<>();

    public Builder studyId(String studyId) {
      this.studyId = studyId;
      return this;
    }

    public Builder cohortId(String cohortId) {
      this.cohortId = cohortId;
      return this;
    }

    public Builder underlayName(String underlayName) {
      this.underlayName = underlayName;
      return this;
    }

    public Builder cohortRevisionGroupId(String cohortRevisionGroupId) {
      this.cohortRevisionGroupId = cohortRevisionGroupId;
      return this;
    }

    public Builder version(int version) {
      this.version = version;
      return this;
    }

    public Builder isMostRecent(boolean isMostRecent) {
      this.isMostRecent = isMostRecent;
      return this;
    }

    public Builder isEditable(boolean isEditable) {
      this.isEditable = isEditable;
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

    public Builder criteriaGroups(List<CriteriaGroup> criteriaGroups) {
      this.criteriaGroups = criteriaGroups;
      return this;
    }

    public Builder addCriteriaGroup(CriteriaGroup criteriaGroup) {
      this.criteriaGroups.add(criteriaGroup);
      return this;
    }

    public String getCohortId() {
      return cohortId;
    }

    public Cohort build() {
      if (cohortId == null) {
        throw new SystemException("Cohort requires id");
      }
      return new Cohort(this);
    }
  }
}
