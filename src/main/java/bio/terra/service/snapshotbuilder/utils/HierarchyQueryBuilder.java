package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.SelectAlias;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SourceVariable;
import bio.terra.service.snapshotbuilder.query.SubQueryPointer;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.tables.Concept;
import bio.terra.service.snapshotbuilder.query.tables.ConceptAncestor;
import bio.terra.service.snapshotbuilder.query.tables.ConceptRelationship;
import bio.terra.service.snapshotbuilder.query.tables.DomainOccurrence;
import bio.terra.service.snapshotbuilder.query.tables.Table;
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
    FieldVariable relationshipId = conceptRelationship.relationshipId();
    FieldVariable parentId = conceptRelationship.conceptId1();
    FieldVariable childId = conceptRelationship.conceptId2();
    Concept child = Concept.joinConceptId(childId);
    Concept parent = Concept.joinConceptId(parentId);
    FieldVariable conceptName = child.name();
    FieldVariable conceptCode = child.code();

    SourceVariable joinHasChildren = makeHasChildrenJoin(childId);

    // To get the total occurrence count for a child concept, we need to join the child through the
    // ancestor table to find all of its children. We don't need to use a left join here
    // because every concept has itself as an ancestor, so there will be at least one match.
    ConceptAncestor conceptAncestor = ConceptAncestor.joinAncestor(childId);

    DomainOccurrence domainOccurrence =
        DomainOccurrence.leftJoinOn(domainOption, conceptAncestor.descendantConceptId());

    // Filter concepts to only return direct children of the given concept
    SourceVariable joinFilter = joinToFilterConcepts(parentId, conceptId);

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
                ConceptAncestor.selectHasChildren(joinHasChildren)))
        .tables(
            List.of(
                conceptRelationship,
                child,
                parent,
                new Table(joinHasChildren),
                conceptAncestor,
                domainOccurrence,
                new Table(joinFilter)))
        .where(
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(relationshipId, new Literal("Subsumes")),
                parent.requireStandardConcept(),
                child.requireStandardConcept()))
        .groupBy(List.of(conceptName, parentId, childId, conceptCode))
        .orderBy(List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING)))
        .build();
  }
  /**
   * Filter concept to only allow standard concepts. See <a
   * href="https://www.ohdsi.org/web/wiki/doku.php?id=documentation:vocabulary:standard_classification_and_source_concepts">Standard,
   * Classification, and Source Concepts</a>
   */
  static SourceVariable joinToFilterConcepts(FieldVariable parentId, int conceptId) {
    ConceptAncestor conceptAncestor = ConceptAncestor.asPrimary();
    FieldVariable ancestorConceptId = conceptAncestor.ancestorConceptId();
    FieldVariable descendantConceptId = conceptAncestor.descendantConceptId();
    var conceptAncestorSubquery =
        new Query.Builder()
            .select(List.of(ancestorConceptId, descendantConceptId))
            .tables(List.of(conceptAncestor))
            .where(
                BooleanAndOrFilterVariable.and(
                    BinaryFilterVariable.equals(descendantConceptId, new Literal(conceptId)),
                    BinaryFilterVariable.notEquals(ancestorConceptId, new Literal(conceptId))))
            .build();
    var conceptAncestorSubqueryPointer =
        new SubQueryPointer(conceptAncestorSubquery, "concept_ancestor_subquery");
    return SourceVariable.forJoined(
        conceptAncestorSubqueryPointer, ConceptAncestor.ANCESTOR_CONCEPT_ID, parentId);
  }

  /** Generate a join clause that is used to determine if the outerConcept has any children. */
  static SourceVariable makeHasChildrenJoin(FieldVariable childId) {
    ConceptAncestor conceptAncestor = ConceptAncestor.asPrimary();
    FieldVariable ancestorConceptId = conceptAncestor.ancestorConceptId();
    FieldVariable descendantConceptId = conceptAncestor.descendantConceptId();
    var minLevelsSeparation = conceptAncestor.minLevelsOfSeparation();
    Concept conceptAncestorJoin = Concept.joinConceptId(descendantConceptId);

    var subquery =
        new Query.Builder()
            .select(List.of(ancestorConceptId, descendantConceptId, minLevelsSeparation))
            .tables(List.of(conceptAncestor, conceptAncestorJoin))
            .where(
                BinaryFilterVariable.equals(
                    conceptAncestorJoin.standardConcept(), new Literal("S")))
            .build();

    var subQueryPointer = new SubQueryPointer(subquery, "has_children");
    var joinHasChildren =
        SourceVariable.forLeftJoined(subQueryPointer, ConceptAncestor.ANCESTOR_CONCEPT_ID, childId);
    joinHasChildren.addJoinClause(
        ConceptAncestor.DESCENDANT_CONCEPT_ID,
        childId,
        BinaryFilterVariable.BinaryOperator.NOT_EQUALS);
    // Ancestors can have children at a minimum of 0 OR 1 levels of separation
    joinHasChildren.addJoinClause(
        ConceptAncestor.MIN_LEVELS_OF_SEPARATION,
        new Literal(1),
        BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL);
    return joinHasChildren;
  }
}
