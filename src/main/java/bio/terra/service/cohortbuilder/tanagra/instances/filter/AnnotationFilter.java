package bio.terra.service.cohortbuilder.tanagra.instances.filter;

import bio.terra.service.cohortbuilder.tanagra.artifact.Annotation;
import bio.terra.service.cohortbuilder.tanagra.artifact.AnnotationValue;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.filtervariable.BinaryFilterVariable.BinaryOperator;
import java.util.List;

public class AnnotationFilter {
  private final Annotation annotation;
  private final BinaryOperator operator;
  private final Literal value;

  public AnnotationFilter(Annotation annotation, BinaryOperator operator, Literal value) {
    this.annotation = annotation;
    this.operator = operator;
    this.value = value;
  }

  public boolean isMatch(List<AnnotationValue> annotationValues) {
    return annotationValues.stream()
        .anyMatch(
            av -> {
              if (!av.getAnnotationId().equals(annotation.getAnnotationId())) {
                return false;
              }
              int comparison = av.getLiteral().compareTo(value);
              switch (operator) {
                case EQUALS:
                  return comparison == 0;
                case NOT_EQUALS:
                  return comparison != 0;
                case LESS_THAN:
                  return comparison == -1;
                case GREATER_THAN:
                  return comparison == 1;
                case LESS_THAN_OR_EQUAL:
                  return comparison <= 0;
                case GREATER_THAN_OR_EQUAL:
                  return comparison >= 0;
                default:
                  throw new SystemException("Unsupported annotation filter operator: " + operator);
              }
            });
  }
}
