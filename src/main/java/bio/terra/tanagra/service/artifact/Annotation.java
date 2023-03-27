package bio.terra.tanagra.service.artifact;

import bio.terra.common.exception.BadRequestException;
import bio.terra.tanagra.query.Literal;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class Annotation {
  private final String cohortId;
  private final String annotationId;
  private final @Nullable
  String displayName;
  private final @Nullable String description;
  private final Literal.DataType dataType;
  private final List<String> enumVals;

  private Annotation(Builder builder) {
    this.cohortId = builder.cohortId;
    this.annotationId = builder.annotationId;
    this.displayName = builder.displayName;
    this.description = builder.description;
    this.dataType = builder.dataType;
    this.enumVals = builder.enumVals;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Unique (per study) identifier of the cohort this annotation belongs to. */
  public String getCohortId() {
    return cohortId;
  }

  /** Unique (per cohort) identifier of this annotation. */
  public String getAnnotationId() {
    return annotationId;
  }

  /** Optional display name of this annotation. */
  public String getDisplayName() {
    return displayName;
  }

  /** Optional description of this annotation. */
  public String getDescription() {
    return description;
  }

  /** Data type of the annotation. */
  public Literal.DataType getDataType() {
    return dataType;
  }

  /** List of enum values for the annotation. */
  public List<String> getEnumVals() {
    return enumVals == null ? Collections.emptyList() : Collections.unmodifiableList(enumVals);
  }

  public static class Builder {
    private String cohortId;
    private String annotationId;
    private @Nullable String displayName;
    private @Nullable String description;
    private Literal.DataType dataType;
    private List<String> enumVals;

    public Builder cohortId(String cohortId) {
      this.cohortId = cohortId;
      return this;
    }

    public Builder annotationId(String annotationId) {
      this.annotationId = annotationId;
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

    public Builder dataType(Literal.DataType dataType) {
      this.dataType = dataType;
      return this;
    }

    public Builder enumVals(List<String> enumVals) {
      this.enumVals = enumVals;
      return this;
    }

    public String getCohortId() {
      return cohortId;
    }

    public Annotation build() {
      if (enumVals != null && !enumVals.isEmpty() && !dataType.equals(Literal.DataType.STRING)) {
        throw new BadRequestException("Enum values are only supported for the STRING data type.");
      }
      return new Annotation(this);
    }
  }
}
