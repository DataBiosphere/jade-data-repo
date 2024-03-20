package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.snapshotbuilder.SelectAlias;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class HierarchyQueryBuilder {

  // Fields in CONCEPT
  private static final String CONCEPT = "concept";
  public static final String CONCEPT_ID = "concept_id";
  public static final String CONCEPT_NAME = "concept_name";
  public static final String CONCEPT_CODE = "concept_code";
  public static final String PARENT_ID = "parent_id";
  private final TableNameGenerator tableNameGenerator;
  final SnapshotBuilderSettings snapshotBuilderSettings;

  protected HierarchyQueryBuilder(
      TableNameGenerator tableNameGenerator, SnapshotBuilderSettings snapshotBuilderSettings) {
    this.tableNameGenerator = tableNameGenerator;
    this.snapshotBuilderSettings = snapshotBuilderSettings;
  }

  public Query generateQuery(int conceptId) {
    var conceptRelationship =
        TableVariable.forPrimary(
            TablePointer.fromTableName("concept_relationship", tableNameGenerator));
    var relationshipId = conceptRelationship.makeFieldVariable("relationship_id");
    var conceptId1 = conceptRelationship.makeFieldVariable("concept_id_1");
    var conceptId2 = conceptRelationship.makeFieldVariable("concept_id_2");
    var child =
        TableVariable.forJoined(
            TablePointer.fromTableName(CONCEPT, tableNameGenerator), CONCEPT_ID, conceptId2);
    var parent =
        TableVariable.forJoined(
            TablePointer.fromTableName(CONCEPT, tableNameGenerator), CONCEPT_ID, conceptId1);
    // SELECT cr.concept_id_1 AS parent_id, cr.concept_id_2 AS concept_id, child.concept_name,
    // child.concept_code
    // FROM concept_relationship AS cr, concept AS child, concept AS parent
    // WHERE cr.concept_id_1 IN (:all parents of conceptId:) AND cr.relationship_id = 'Subsumes'
    // AND parent.standard_concept = 'S' AND child.standard_concept = 'S'
    return new Query(
        List.of(
            new SelectAlias(conceptId1, PARENT_ID),
            new SelectAlias(conceptId2, CONCEPT_ID),
            child.makeFieldVariable(CONCEPT_NAME),
            child.makeFieldVariable(CONCEPT_CODE)),
        List.of(conceptRelationship, child, parent),
        BooleanAndOrFilterVariable.and(
            SubQueryFilterVariable.in(conceptId1, selectAllParents(conceptId)),
            BinaryFilterVariable.equals(relationshipId, new Literal("Subsumes")),
            requireStandardConcept(parent),
            requireStandardConcept(child)));
  }

  /** Generate a filter constraint on concept to only allow standard concepts. */
  private static BinaryFilterVariable requireStandardConcept(TableVariable concept) {
    return BinaryFilterVariable.equals(
        concept.makeFieldVariable("standard_concept"), new Literal("S"));
  }

  /** Given a concept ID, select all of its parent concept IDs. */
  private Query selectAllParents(int conceptId) {
    // SELECT ancestor_concept_id FROM concept_ancestor WHERE descendant_concept_id = :conceptId:
    var conceptAncestor =
        TableVariable.forPrimary(
            TablePointer.fromTableName("concept_ancestor", tableNameGenerator));
    return new Query(
        List.of(conceptAncestor.makeFieldVariable("ancestor_concept_id")),
        List.of(conceptAncestor),
        BinaryFilterVariable.equals(
            conceptAncestor.makeFieldVariable("descendant_concept_id"), new Literal(conceptId)));
  }
}
