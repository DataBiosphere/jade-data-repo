package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.util.HashMap;
import java.util.Map;

public class ConceptAncestor extends TableVariable {

  public static final String TABLE_NAME = "concept_ancestor";
  public static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  public static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);
  private final Map<String, FieldVariable> fields = new HashMap<>();

  public ConceptAncestor() {
    super(tablePointer, null, null, false);
  }

  public ConceptAncestor(TableVariable.Builder tableVariableBuilder) {
    super(
        tablePointer,
        tableVariableBuilder.getJoinField(),
        tableVariableBuilder.getJoinFieldOnParent(),
        tableVariableBuilder.isLeftJoin());
  }

  private FieldVariable getFieldVariable(String fieldName) {
    return fields.computeIfAbsent(fieldName, this::makeFieldVariable);
  }

  public FieldVariable ancestor_concept_id() {
    return getFieldVariable(ANCESTOR_CONCEPT_ID);
  }

  public FieldVariable descendant_concept_id() {
    return getFieldVariable(DESCENDANT_CONCEPT_ID);
  }
}
