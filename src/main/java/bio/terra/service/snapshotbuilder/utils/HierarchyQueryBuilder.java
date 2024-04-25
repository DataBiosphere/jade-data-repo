package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.SelectAlias;
import bio.terra.service.snapshotbuilder.query.ExistsExpression;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import bio.terra.service.snapshotbuilder.utils.constants.Concept;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptAncestor;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptRelationship;
import bio.terra.service.snapshotbuilder.utils.constants.Person;
import java.util.List;

public class HierarchyQueryBuilder {

  /**
   * Generate a query to find all parent concepts of a given concept, and for each parent to find
   * all its children.
   *
   * <pre>{@code
   * SELECT cr.concept_id_1 AS parent_id, cr.concept_id_2 AS concept_id, child.concept_name, child.concept_code, has_children, count
   *   FROM concept_relationship cr, concept AS child, concept AS parent
   *   WHERE cr.concept_id_1 IN (:all parents of conceptId:) AND cr.relationship_id = 'Subsumes'
   *   AND parent.concept_id = cr.concept_id_1 AND child.concept_id = cr.concept_id_2
   *   AND parent.standard_concept = 'S' AND child.standard_concept = 'S'
   * }</pre>
   */
  public Query generateQuery(SnapshotBuilderDomainOption domainOption, int conceptId) {
    var conceptRelationship =
        TableVariable.forPrimary(TablePointer.fromTableName(ConceptRelationship.TABLE_NAME));
    var relationshipId = conceptRelationship.makeFieldVariable(ConceptRelationship.RELATIONSHIP_ID);
    var parentId = conceptRelationship.makeFieldVariable(ConceptRelationship.CONCEPT_ID_1);
    var childId = conceptRelationship.makeFieldVariable(ConceptRelationship.CONCEPT_ID_2);
    var child =
        TableVariable.forJoined(
            TablePointer.fromTableName(Concept.TABLE_NAME), Concept.CONCEPT_ID, childId);
    var parent =
        TableVariable.forJoined(
            TablePointer.fromTableName(Concept.TABLE_NAME), Concept.CONCEPT_ID, parentId);
    FieldVariable conceptName = child.makeFieldVariable(Concept.CONCEPT_NAME);
    FieldVariable conceptCode = child.makeFieldVariable(Concept.CONCEPT_CODE);

    // To get the total occurrence count for a child concept, we need to join the child through the
    // ancestor table to find all of its children. We don't need to use a left join here
    // because every concept has itself as an ancestor, so there will be at least one match.
    var conceptAncestor =
        TableVariable.forJoined(
            TablePointer.fromTableName(ConceptAncestor.TABLE_NAME),
            ConceptAncestor.ANCESTOR_CONCEPT_ID,
            childId);

    TableVariable domainOccurrence =
        TableVariable.forLeftJoined(
            TablePointer.fromTableName(domainOption.getTableName()),
            domainOption.getColumnName(),
            conceptAncestor.makeFieldVariable(ConceptAncestor.DESCENDANT_CONCEPT_ID));

    // COUNT(DISTINCT person_id)
    FieldVariable count =
        domainOccurrence.makeFieldVariable(
            Person.PERSON_ID, "COUNT", QueryBuilderFactory.COUNT, true);

    return new Query(
        List.of(
            new SelectAlias(parentId, QueryBuilderFactory.PARENT_ID),
            new SelectAlias(childId, Concept.CONCEPT_ID),
            conceptName,
            conceptCode,
            count,
            hasChildrenExpression(childId)),
        List.of(conceptRelationship, child, parent, conceptAncestor, domainOccurrence),
        BooleanAndOrFilterVariable.and(
            SubQueryFilterVariable.in(parentId, selectAllParents(conceptId)),
            BinaryFilterVariable.equals(relationshipId, new Literal("Subsumes")),
            requireStandardConcept(parent),
            requireStandardConcept(child)),
        List.of(conceptName, parentId, childId, conceptCode),
        List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING)));
  }

  /**
   * Filter concept to only allow standard concepts. See <a
   * href="https://www.ohdsi.org/web/wiki/doku.php?id=documentation:vocabulary:standard_classification_and_source_concepts">Standard,
   * Classification, and Source Concepts</a>
   */
  private static BinaryFilterVariable requireStandardConcept(TableVariable concept) {
    return BinaryFilterVariable.equals(
        concept.makeFieldVariable(Concept.STANDARD_CONCEPT), new Literal("S"));
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
    var conceptAncestor =
        TableVariable.forPrimary(TablePointer.fromTableName(ConceptAncestor.TABLE_NAME));
    var conceptIdLiteral = new Literal(conceptId);
    FieldVariable ancestorConceptId =
        conceptAncestor.makeFieldVariable(ConceptAncestor.ANCESTOR_CONCEPT_ID);
    return new Query(
        List.of(ancestorConceptId),
        List.of(conceptAncestor),
        BooleanAndOrFilterVariable.and(
            BinaryFilterVariable.equals(
                conceptAncestor.makeFieldVariable(ConceptAncestor.DESCENDANT_CONCEPT_ID),
                conceptIdLiteral),
            BinaryFilterVariable.notEquals(ancestorConceptId, conceptIdLiteral)));
  }

  /**
   * Generate a subquery that returns true if the outerConcept has any children.
   *
   * <pre>{@code
   * EXISTS (SELECT
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
   * }</pre>
   */
  static SelectExpression hasChildrenExpression(FieldVariable conceptId) {
    var conceptAncestor =
        TableVariable.forPrimary(TablePointer.fromTableName(ConceptAncestor.TABLE_NAME));
    var descendantConceptId =
        conceptAncestor.makeFieldVariable(ConceptAncestor.DESCENDANT_CONCEPT_ID);
    var innerConcept =
        TableVariable.forJoined(
            TablePointer.fromTableName(Concept.TABLE_NAME),
            Concept.CONCEPT_ID,
            descendantConceptId);
    return new ExistsExpression(
        new Query(
            List.of(new Literal(1)),
            List.of(conceptAncestor, innerConcept),
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(
                    conceptAncestor.makeFieldVariable(ConceptAncestor.ANCESTOR_CONCEPT_ID),
                    conceptId),
                BinaryFilterVariable.notEquals(descendantConceptId, conceptId),
                requireStandardConcept(innerConcept))),
        QueryBuilderFactory.HAS_CHILDREN);
  }
}
