package bio.terra.service.snapshotbuilder.query.tables;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;

public class ConceptAncestor extends Table {

  public static final String TABLE_NAME = "concept_ancestor";
  public static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  public static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";
  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);

  private ConceptAncestor() {
    super(TableVariable.forPrimary(tablePointer));
  }

  private ConceptAncestor(String joinField, FieldVariable joinFieldOnParent) {
    super(TableVariable.forJoined(tablePointer, joinField, joinFieldOnParent));
  }

  public static ConceptAncestor asPrimary() {
    return new ConceptAncestor();
  }

  public static ConceptAncestor joinDescendant(FieldVariable onParent) {
    return new ConceptAncestor(DESCENDANT_CONCEPT_ID, onParent);
  }

  public static ConceptAncestor joinAncestor(FieldVariable onParent) {
    return new ConceptAncestor(ANCESTOR_CONCEPT_ID, onParent);
  }

  public FieldVariable ancestor_concept_id() {
    return getFieldVariable(ANCESTOR_CONCEPT_ID);
  }

  public FieldVariable descendant_concept_id() {
    return getFieldVariable(DESCENDANT_CONCEPT_ID);
  }
}
