package bio.terra.service.snapshotbuilder.utils;

import bio.terra.service.snapshotbuilder.SelectAlias;
import bio.terra.service.snapshotbuilder.query.ExistsExpression;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class HierarchyQueryBuilder {

  public static final String HAS_CHILDREN = "has_children";
  // Fields in CONCEPT
  private static final String CONCEPT = "concept";
  public static final String CONCEPT_ID = "concept_id";
  public static final String CONCEPT_NAME = "concept_name";
  public static final String CONCEPT_CODE = "concept_code";
  public static final String STANDARD_CONCEPT = "standard_concept";

  public static final String PARENT_ID = "parent_id";

  // CONCEPT_ANCESTOR table
  static final String CONCEPT_ANCESTOR = "concept_ancestor";
  static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";

  /**
   * Generate a query to find all parent concepts of a given concept, and for each parent to find
   * all its children.
   *
   * <pre>{@code
   * SELECT cr.concept_id_1 AS parent_id, cr.concept_id_2 AS concept_id, child.concept_name, child.concept_code
   *   FROM concept_relationship cr, concept AS child, concept AS parent
   *   WHERE cr.concept_id_1 IN (:all parents of conceptId:) AND cr.relationship_id = 'Subsumes'
   *   AND parent.concept_id = cr.concept_id_1 AND child.concept_id = cr.concept_id_2
   *   AND parent.standard_concept = 'S' AND child.standard_concept = 'S'
   * }</pre>
   */
  public Query generateQuery(int conceptId) {
    var conceptRelationship =
        TableVariable.forPrimary(TablePointer.fromTableName("concept_relationship"));
    var relationshipId = conceptRelationship.makeFieldVariable("relationship_id");
    var conceptId1 = conceptRelationship.makeFieldVariable("concept_id_1");
    var conceptId2 = conceptRelationship.makeFieldVariable("concept_id_2");
    var child =
        TableVariable.forJoined(TablePointer.fromTableName(CONCEPT), CONCEPT_ID, conceptId2);
    var parent =
        TableVariable.forJoined(TablePointer.fromTableName(CONCEPT), CONCEPT_ID, conceptId1);
    return new Query(
        List.of(
            new SelectAlias(conceptId1, PARENT_ID),
            new SelectAlias(conceptId2, CONCEPT_ID),
            child.makeFieldVariable(CONCEPT_NAME),
            child.makeFieldVariable(CONCEPT_CODE),
            hasChildrenExpression(child)),
        List.of(conceptRelationship, child, parent),
        BooleanAndOrFilterVariable.and(
            SubQueryFilterVariable.in(conceptId1, selectAllParents(conceptId)),
            BinaryFilterVariable.equals(relationshipId, new Literal("Subsumes")),
            requireStandardConcept(parent),
            requireStandardConcept(child)));
  }

  /**
   * Filter concept to only allow standard concepts. See <a
   * href="https://www.ohdsi.org/web/wiki/doku.php?id=documentation:vocabulary:standard_classification_and_source_concepts">Standard,
   * Classification, and Source Concepts</a>
   */
  static BinaryFilterVariable requireStandardConcept(TableVariable concept) {
    return BinaryFilterVariable.equals(
        concept.makeFieldVariable(STANDARD_CONCEPT), new Literal("S"));
  }

  /**
   * Given a concept ID, select all of its parent concept IDs. Note that a concept may be its own
   * ancestor so that case must be excluded.
   *
   * <pre>
   * {@code SELECT ancestor_concept_id FROM concept_ancestor
   * WHERE descendant_concept_id = :conceptId: AND ancestor_concept_id != :conceptId:;}
   * </pre>
   */
  private Query selectAllParents(int conceptId) {
    var conceptAncestor = TableVariable.forPrimary(TablePointer.fromTableName(CONCEPT_ANCESTOR));
    var conceptIdLiteral = new Literal(conceptId);
    FieldVariable ancestorConceptId = conceptAncestor.makeFieldVariable(ANCESTOR_CONCEPT_ID);
    return new Query(
        List.of(ancestorConceptId),
        List.of(conceptAncestor),
        BooleanAndOrFilterVariable.and(
            BinaryFilterVariable.equals(
                conceptAncestor.makeFieldVariable(DESCENDANT_CONCEPT_ID), conceptIdLiteral),
            BinaryFilterVariable.notEquals(ancestorConceptId, conceptIdLiteral)));
  }

  /**
   * Generate a subquery that returns true if the outerConcept has any children.
   *
   * <pre>{@code
   * EXISTS(SELECT
   *     1
   *   FROM
   *     concept_ancestor ca
   *   JOIN
   *     concept c2
   *   ON
   *     c2.concept_id = ca.descendant_concept_id
   *     AND ca.ancestor_concept_id = c.concept_id
   *     AND ca.descendant_concept_id != c.concept_id
   *     AND c2.standard_concept = 'S') AS has_children
   *     </pre>
   */
  static SelectExpression hasChildrenExpression(TableVariable outerConcept) {
    var conceptId = outerConcept.makeFieldVariable(CONCEPT_ID);
    var conceptAncestor = TableVariable.forPrimary(TablePointer.fromTableName(CONCEPT_ANCESTOR));
    var descendantConceptId = conceptAncestor.makeFieldVariable(DESCENDANT_CONCEPT_ID);
    var innerConcept =
        TableVariable.forJoined(
            TablePointer.fromTableName(CONCEPT), CONCEPT_ID, descendantConceptId);
    return new ExistsExpression(
        new Query(
            List.of(new Literal(1)),
            List.of(conceptAncestor, innerConcept),
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(
                    conceptAncestor.makeFieldVariable(ANCESTOR_CONCEPT_ID), conceptId),
                BinaryFilterVariable.notEquals(descendantConceptId, conceptId),
                requireStandardConcept(innerConcept))),
        HAS_CHILDREN);
  }
}
