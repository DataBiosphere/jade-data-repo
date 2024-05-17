package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class Concept extends TableVariable {

  public static final String TABLE_NAME = "concept";
  public static final String DOMAIN_ID = "domain_id";
  public static final String CONCEPT_NAME = "concept_name";
  public static final String CONCEPT_ID = "concept_id";
  public static final String STANDARD_CONCEPT = "standard_concept";
  public static final String CONCEPT_CODE = "concept_code";
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);

  private final Map<String, FieldVariable> fields = new HashMap<>();

  private Concept(
      TablePointer tablePointer,
      @Nullable String joinField,
      @Nullable FieldVariable joinFieldOnParent,
      boolean isLeftJoin) {
    super(tablePointer, joinField, joinFieldOnParent, isLeftJoin);
  }

  private FieldVariable getFieldVariable(String fieldName) {
    return fields.computeIfAbsent(fieldName, this::makeFieldVariable);
  }

  public FieldVariable name() {
    return getFieldVariable(CONCEPT_NAME);
  }

  public FieldVariable concept_id() {
    return getFieldVariable(CONCEPT_ID);
  }

  public FieldVariable domain_id() {
    return getFieldVariable(DOMAIN_ID);
  }

  public FieldVariable code() {
    return getFieldVariable(CONCEPT_CODE);
  }

  public FieldVariable standardConcept() {
    return getFieldVariable(STANDARD_CONCEPT);
  }

  public static class Builder extends TableVariable.Builder<Concept> {
    public Concept build() {
      return new Concept(tablePointer, getJoinField(), getJoinFieldOnParent(), isLeftJoin());
    }
  }
}
