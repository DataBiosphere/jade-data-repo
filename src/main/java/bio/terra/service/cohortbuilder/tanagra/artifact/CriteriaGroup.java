package bio.terra.service.cohortbuilder.tanagra.artifact;

import bio.terra.tanagra.query.filtervariable.BooleanAndOrFilterVariable.LogicalOperator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Internal representation of a Criteria Group.
 *
 * <p>A criteria group is a collection of criteria and boolean logic operator(s) that define a
 * cohort or concept set.
 */
public class CriteriaGroup {
  private final String cohortId;
  private final String criteriaGroupId;
  private final String userFacingCriteriaGroupId;
  private final @Nullable String displayName;
  private final LogicalOperator operator;
  private final boolean isExcluded;
  private final List<Criteria> criterias;

  private CriteriaGroup(Builder builder) {
    this.cohortId = builder.cohortId;
    this.criteriaGroupId = builder.criteriaGroupId;
    this.userFacingCriteriaGroupId = builder.userFacingCriteriaGroupId;
    this.displayName = builder.displayName;
    this.operator = builder.operator;
    this.isExcluded = builder.isExcluded;
    this.criterias = builder.criterias;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Unique (per study) identifier of the cohort this criteria group belongs to. */
  public String getCohortId() {
    return cohortId;
  }

  /** Unique (per cohort) identifier of this criteria group. */
  public String getCriteriaGroupId() {
    return criteriaGroupId;
  }

  /** User-defined identifier of this criteria group. */
  public String getUserFacingCriteriaGroupId() {
    return userFacingCriteriaGroupId;
  }

  /** Optional display name for the criteria group. */
  public String getDisplayName() {
    return displayName;
  }

  /** Boolean logic operator to combine the criteria: AND, OR. */
  public LogicalOperator getOperator() {
    return operator;
  }

  /** True to exclude the group, false to include it. */
  public boolean isExcluded() {
    return isExcluded;
  }

  /** List of criteria in the group. */
  public List<Criteria> getCriterias() {
    return Collections.unmodifiableList(criterias);
  }

  public static class Builder {
    private String cohortId;
    private String criteriaGroupId;
    private String userFacingCriteriaGroupId;
    private @Nullable String displayName;
    private LogicalOperator operator;
    private boolean isExcluded;
    private List<Criteria> criterias = new ArrayList<>();

    public Builder cohortId(String cohortId) {
      this.cohortId = cohortId;
      return this;
    }

    public Builder criteriaGroupId(String criteriaGroupId) {
      this.criteriaGroupId = criteriaGroupId;
      return this;
    }

    public Builder userFacingCriteriaGroupId(String userFacingCriteriaGroupId) {
      this.userFacingCriteriaGroupId = userFacingCriteriaGroupId;
      return this;
    }

    public Builder displayName(String displayName) {
      this.displayName = displayName;
      return this;
    }

    public Builder operator(LogicalOperator operator) {
      this.operator = operator;
      return this;
    }

    public Builder isExcluded(boolean isExcluded) {
      this.isExcluded = isExcluded;
      return this;
    }

    public Builder criterias(List<Criteria> criterias) {
      this.criterias = criterias;
      return this;
    }

    public Builder addCriteria(Criteria criteria) {
      this.criterias.add(criteria);
      return this;
    }

    public String getCriteriaGroupId() {
      return criteriaGroupId;
    }

    public CriteriaGroup build() {
      return new CriteriaGroup(this);
    }
  }
}
