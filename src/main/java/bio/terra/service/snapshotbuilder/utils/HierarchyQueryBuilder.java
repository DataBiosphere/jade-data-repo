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
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import bio.terra.service.snapshotbuilder.query.tables.Concept;
import bio.terra.service.snapshotbuilder.query.tables.ConceptAncestor;
import bio.terra.service.snapshotbuilder.query.tables.ConceptRelationship;
import bio.terra.service.snapshotbuilder.query.tables.DomainOccurrence;
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
    ConceptRelationship conceptRelationship = ConceptRelationship.asPrimary();
    var relationshipId = conceptRelationship.relationshipId();
    var parentId = conceptRelationship.conceptId1();
    var childId = conceptRelationship.conceptId2();
    Concept child = Concept.conceptId(childId);
    var parent = Concept.conceptId(parentId);
    FieldVariable conceptName = child.name();
    FieldVariable conceptCode = child.code();

    // To get the total occurrence count for a child concept, we need to join the child through the
    // ancestor table to find all of its children. We don't need to use a left join here
    // because every concept has itself as an ancestor, so there will be at least one match.
    var conceptAncestor = ConceptAncestor.joinAncestor(childId);

    DomainOccurrence domainOccurrence =
        DomainOccurrence.leftJoinOn(domainOption, conceptAncestor.descendantConceptId());

    // COUNT(DISTINCT person_id)
    FieldVariable personCount = domainOccurrence.countPersonId();

    return new Query.Builder()
        .select(
            List.of(
                new SelectAlias(parentId, QueryBuilderFactory.PARENT_ID),
                new SelectAlias(childId, Concept.CONCEPT_ID),
                conceptName,
                conceptCode,
                personCount,
                hasChildrenExpression(childId)))
        .tables(List.of(conceptRelationship, child, parent, conceptAncestor, domainOccurrence))
        .where(
            BooleanAndOrFilterVariable.and(
                SubQueryFilterVariable.in(parentId, selectAllParents(conceptId)),
                BinaryFilterVariable.equals(relationshipId, new Literal("Subsumes")),
                requireStandardConcept(parent.standardConcept()),
                requireStandardConcept(child.standardConcept())))
        .groupBy(List.of(conceptName, parentId, childId, conceptCode))
        .orderBy(List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING)))
        .build();
  }
  /**
   * Filter concept to only allow standard concepts. See <a
   * href="https://www.ohdsi.org/web/wiki/doku.php?id=documentation:vocabulary:standard_classification_and_source_concepts">Standard,
   * Classification, and Source Concepts</a>
   */
  private static BinaryFilterVariable requireStandardConcept(FieldVariable standardConcept) {
    return BinaryFilterVariable.equals(standardConcept, new Literal("S"));
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
    ConceptAncestor conceptAncestor = ConceptAncestor.asPrimary();
    var conceptIdLiteral = new Literal(conceptId);
    FieldVariable ancestorConceptId = conceptAncestor.ancestorConceptId();
    return new Query.Builder()
        .select(List.of(ancestorConceptId))
        .tables(List.of(conceptAncestor))
        .where(
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(
                    conceptAncestor.descendantConceptId(), conceptIdLiteral),
                BinaryFilterVariable.notEquals(ancestorConceptId, conceptIdLiteral)))
        .build();
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
    ConceptAncestor conceptAncestor = ConceptAncestor.asPrimary();
    FieldVariable descendantConceptId = conceptAncestor.descendantConceptId();
    Concept innerConcept = Concept.conceptId(descendantConceptId);
    return new ExistsExpression(
        new Query.Builder()
            .select(List.of(new Literal(1)))
            .tables(List.of(conceptAncestor, innerConcept))
            .where(
                BooleanAndOrFilterVariable.and(
                    BinaryFilterVariable.equals(conceptAncestor.ancestorConceptId(), conceptId),
                    BinaryFilterVariable.notEquals(descendantConceptId, conceptId),
                    requireStandardConcept(innerConcept.standardConcept())))
            .build(),
        QueryBuilderFactory.HAS_CHILDREN);
  }
}
