package bio.terra.service.cohortbuilder.tanagra.artifact;

import bio.terra.tanagra.query.Literal;

public class AnnotationValue {
  private final String reviewId;
  private final String annotationId;
  private final String annotationValueId;
  private final String entityInstanceId;
  private final Literal literal;

  private AnnotationValue(Builder builder) {
    this.reviewId = builder.reviewId;
    this.annotationId = builder.annotationId;
    this.annotationValueId = builder.annotationValueId;
    this.entityInstanceId = builder.entityInstanceId;
    this.literal = builder.literal;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Unique (per cohort) identifier of the review this annotation value belongs to. */
  public String getReviewId() {
    return reviewId;
  }

  /** Unique (per cohort) identifier of the annotation this value is for. */
  public String getAnnotationId() {
    return annotationId;
  }

  /** Unique (per review) identifier of this annotation value. */
  public String getAnnotationValueId() {
    return annotationValueId;
  }

  /** ID of the entity instance that this annotation references. */
  public String getEntityInstanceId() {
    return entityInstanceId;
  }

  /** Literal value of the annotation. */
  public Literal getLiteral() {
    return literal;
  }

  public static class Builder {
    private String reviewId;
    private String annotationId;
    private String annotationValueId;
    private String entityInstanceId;
    private Literal literal;

    public Builder reviewId(String reviewId) {
      this.reviewId = reviewId;
      return this;
    }

    public Builder annotationId(String annotationId) {
      this.annotationId = annotationId;
      return this;
    }

    public Builder annotationValueId(String annotationValueId) {
      this.annotationValueId = annotationValueId;
      return this;
    }

    public Builder entityInstanceId(String entityInstanceId) {
      this.entityInstanceId = entityInstanceId;
      return this;
    }

    public Builder literal(Literal literal) {
      this.literal = literal;
      return this;
    }

    public AnnotationValue build() {
      return new AnnotationValue(this);
    }
  }
}
