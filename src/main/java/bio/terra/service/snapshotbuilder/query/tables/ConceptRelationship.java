package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.TableVariableBuilder;
import java.util.HashMap;
import java.util.Map;

public class ConceptRelationship extends TableVariable {

  public static final String TABLE_NAME = "concept_relationship";
  public static final String CONCEPT_ID_1 = "concept_id_1";
  public static final String CONCEPT_ID_2 = "concept_id_2";
  public static final String RELATIONSHIP_ID = "relationship_id";
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);
  private final Map<String, FieldVariable> fields = new HashMap<>();

  public ConceptRelationship() {
    super(TablePointer.fromTableName(TABLE_NAME), null, null, false);
  }

  public ConceptRelationship(TableVariableBuilder tableVariableBuilder) {
    super(
        tablePointer,
        tableVariableBuilder.getJoinField(),
        tableVariableBuilder.getJoinFieldOnParent(),
        tableVariableBuilder.isLeftJoin());
  }

  private FieldVariable getFieldVariable(String fieldName) {
    return fields.computeIfAbsent(fieldName, this::makeFieldVariable);
  }

  public FieldVariable concept_id_1() {
    return getFieldVariable(CONCEPT_ID_1);
  }

  public FieldVariable concept_id_2() {
    return getFieldVariable(CONCEPT_ID_2);
  }

  public FieldVariable relationship_id() {
    return getFieldVariable(RELATIONSHIP_ID);
  }
}
