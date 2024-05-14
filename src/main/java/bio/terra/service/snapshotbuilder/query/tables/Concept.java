package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
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

  public Concept() {
    super(tablePointer, null, null, false);
  }

  public Concept(TableVariable.Builder tableVariableBuilder) {
    super(
        tablePointer,
        tableVariableBuilder.getJoinField(),
        tableVariableBuilder.getJoinFieldOnParent(),
        tableVariableBuilder.isLeftJoin());
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

  public FieldVariable standard_concept() {
    return getFieldVariable(STANDARD_CONCEPT);
  }
}
