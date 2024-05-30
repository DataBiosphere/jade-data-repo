package bio.terra.service.snapshotbuilder.query.table;

import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;

public class ConceptAncestor extends Table {

  public static final String TABLE_NAME = "concept_ancestor";
  public static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  public static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";
  public static final String MIN_LEVELS_OF_SEPARATION = "min_levels_of_separation";

  private static final TablePointer tablePointer = TablePointer.fromTableName(TABLE_NAME);

  private ConceptAncestor() {
    super(SourceVariable.forPrimary(tablePointer));
  }

  private ConceptAncestor(String joinField, FieldVariable joinFieldOnParent) {
    super(SourceVariable.forJoined(tablePointer, joinField, joinFieldOnParent));
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

  public FieldVariable ancestorConceptId() {
    return getFieldVariable(ANCESTOR_CONCEPT_ID);
  }

  public FieldVariable descendantConceptId() {
    return getFieldVariable(DESCENDANT_CONCEPT_ID);
  }

  public FieldVariable minLevelsOfSeparation() {
    return getFieldVariable(MIN_LEVELS_OF_SEPARATION);
  }

  public static FieldVariable selectHasChildren(SourceVariable joinHasChildren) {
    return joinHasChildren.makeFieldVariable(DESCENDANT_CONCEPT_ID, "COUNT", "has_children", true);
  }
}
