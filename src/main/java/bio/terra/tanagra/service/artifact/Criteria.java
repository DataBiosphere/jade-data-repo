package bio.terra.tanagra.service.artifact;

import javax.annotation.Nullable;

/**
 * Internal representation of a Criteria.
 *
 * <p>A criteria is a single selection that defines a cohort or concept set.
 */
public class Criteria {
  private final String criteriaGroupId;
  private final String conceptSetId;
  private final String criteriaId;
  private final String userFacingCriteriaId;
  private final @Nullable String displayName;
  private final String pluginName;
  private final String selectionData;
  private final String uiConfig;

  private Criteria(Builder builder) {
    this.criteriaGroupId = builder.criteriaGroupId;
    this.conceptSetId = builder.conceptSetId;
    this.criteriaId = builder.criteriaId;
    this.userFacingCriteriaId = builder.userFacingCriteriaId;
    this.displayName = builder.displayName;
    this.pluginName = builder.pluginName;
    this.selectionData = builder.selectionData;
    this.uiConfig = builder.uiConfig;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Unique (per cohort) identifier of the criteria group this criteria belongs to. */
  public String getCriteriaGroupId() {
    return criteriaGroupId;
  }

  /** Unique (per study) identifier of the concept set this criteria belongs to. */
  public String getConceptSetId() {
    return conceptSetId;
  }

  /** Unique (per criteria group) identifier of this criteria. */
  public String getCriteriaId() {
    return criteriaId;
  }

  /** User-defined identifier of this criteria group. */
  public String getUserFacingCriteriaId() {
    return userFacingCriteriaId;
  }

  /** Optional display name for the criteria. */
  public String getDisplayName() {
    return displayName;
  }

  /** Name of the plugin that generated this criteria. */
  public String getPluginName() {
    return pluginName;
  }

  /** Serialized (JSON-format) plugin-specific representation of the user's selection. */
  public String getSelectionData() {
    return selectionData;
  }

  /** Serialized (JSON-format) plugin-specific UI configuration for the criteria. */
  public String getUiConfig() {
    return uiConfig;
  }

  public static class Builder {
    private String criteriaGroupId;
    private String conceptSetId;
    private String criteriaId;
    private String userFacingCriteriaId;
    private @Nullable String displayName;
    private String pluginName;
    private String selectionData;
    private String uiConfig;

    public Builder criteriaGroupId(String criteriaGroupId) {
      this.criteriaGroupId = criteriaGroupId;
      return this;
    }

    public Builder conceptSetId(String conceptSetId) {
      this.conceptSetId = conceptSetId;
      return this;
    }

    public Builder criteriaId(String criteriaId) {
      this.criteriaId = criteriaId;
      return this;
    }

    public Builder userFacingCriteriaId(String userFacingCriteriaId) {
      this.userFacingCriteriaId = userFacingCriteriaId;
      return this;
    }

    public Builder displayName(String displayName) {
      this.displayName = displayName;
      return this;
    }

    public Builder pluginName(String pluginName) {
      this.pluginName = pluginName;
      return this;
    }

    public Builder selectionData(String selectionData) {
      this.selectionData = selectionData;
      return this;
    }

    public Builder uiConfig(String uiConfig) {
      this.uiConfig = uiConfig;
      return this;
    }

    public Criteria build() {
      return new Criteria(this);
    }
  }
}
