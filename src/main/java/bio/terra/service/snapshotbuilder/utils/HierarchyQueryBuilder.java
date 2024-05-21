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
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
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
        SourceVariable.forPrimary(TablePointer.fromTableName(ConceptRelationship.TABLE_NAME));
    var relationshipId = conceptRelationship.makeFieldVariable(ConceptRelationship.RELATIONSHIP_ID);
    var parentId = conceptRelationship.makeFieldVariable(ConceptRelationship.CONCEPT_ID_1);
    var childId = conceptRelationship.makeFieldVariable(ConceptRelationship.CONCEPT_ID_2);
    var child =
        SourceVariable.forJoined(
            TablePointer.fromTableName(Concept.TABLE_NAME), Concept.CONCEPT_ID, childId);
    var parent =
        SourceVariable.forJoined(
            TablePointer.fromTableName(Concept.TABLE_NAME), Concept.CONCEPT_ID, parentId);
    FieldVariable conceptName = child.makeFieldVariable(Concept.CONCEPT_NAME);
    FieldVariable conceptCode = child.makeFieldVariable(Concept.CONCEPT_CODE);

    var hasChildrenJoin = hasChildrenJoin(childId);

    // To get the total occurrence count for a child concept, we need to join the child through the
    // ancestor table to find all of its children. We don't need to use a left join here
    // because every concept has itself as an ancestor, so there will be at least one match.
    var conceptAncestor =
        SourceVariable.forJoined(
            TablePointer.fromTableName(ConceptAncestor.TABLE_NAME),
            ConceptAncestor.ANCESTOR_CONCEPT_ID,
            childId);
    SourceVariable domainOccurrence =
        SourceVariable.forLeftJoined(
            TablePointer.fromTableName(domainOption.getTableName()),
            domainOption.getColumnName(),
            conceptAncestor.makeFieldVariable(ConceptAncestor.DESCENDANT_CONCEPT_ID));

    var joinFilter = joinToFilterConcepts(parentId, conceptId);

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
            hasChildrenSelect(hasChildrenJoin)),
        List.of(
            conceptRelationship,
            child,
            parent,
            hasChildrenJoin,
            conceptAncestor,
            domainOccurrence,
            joinFilter),
        BooleanAndOrFilterVariable.and(
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
  private static BinaryFilterVariable requireStandardConcept(SourceVariable concept) {
    return BinaryFilterVariable.equals(
        concept.makeFieldVariable(Concept.STANDARD_CONCEPT), new Literal("S"));
  }

  static SourceVariable joinToFilterConcepts(FieldVariable parentId, int conceptId) {
    var conceptAncestorTable =
        SourceVariable.forPrimary(TablePointer.fromTableName(ConceptAncestor.TABLE_NAME));
    var ancestorConceptId =
        conceptAncestorTable.makeFieldVariable(ConceptAncestor.ANCESTOR_CONCEPT_ID);
    var descendantConceptId =
        conceptAncestorTable.makeFieldVariable(ConceptAncestor.DESCENDANT_CONCEPT_ID);
    var subquery =
        new Query(
            List.of(ancestorConceptId, descendantConceptId),
            List.of(conceptAncestorTable),
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(descendantConceptId, new Literal(conceptId)),
                BinaryFilterVariable.notEquals(ancestorConceptId, new Literal(conceptId))),
            null);
    var subQueryPointer = new SubQueryPointer(subquery, "join_filter");
    return SourceVariable.forJoined(subQueryPointer, ConceptAncestor.ANCESTOR_CONCEPT_ID, parentId);
  }

  static FieldVariable hasChildrenSelect(SourceVariable hasChildrenJoin) {
    return hasChildrenJoin.makeFieldVariable(
        ConceptAncestor.DESCENDANT_CONCEPT_ID, "COUNT", "has_children", "> 0", true);
  }

  /** Generate a join clause that is used to determine if the outerConcept has any children. */
  static SourceVariable hasChildrenJoin(FieldVariable childId) {
    var conceptAncestorTable =
        SourceVariable.forPrimary(TablePointer.fromTableName(ConceptAncestor.TABLE_NAME));
    var ancestorConceptId =
        conceptAncestorTable.makeFieldVariable(ConceptAncestor.ANCESTOR_CONCEPT_ID);
    var descendantConceptId =
        conceptAncestorTable.makeFieldVariable(ConceptAncestor.DESCENDANT_CONCEPT_ID);
    var minLevelsSeparation =
        conceptAncestorTable.makeFieldVariable(ConceptAncestor.MIN_LEVELS_OF_SEPARATION);
    var conceptAncestorJoin =
        SourceVariable.forJoined(
            TablePointer.fromTableName(Concept.TABLE_NAME),
            Concept.CONCEPT_ID,
            descendantConceptId);
    var subquery =
        new Query(
            List.of(ancestorConceptId, descendantConceptId, minLevelsSeparation),
            List.of(conceptAncestorTable, conceptAncestorJoin),
            BinaryFilterVariable.equals(
                conceptAncestorJoin.makeFieldVariable(Concept.STANDARD_CONCEPT), new Literal("S")),
            null);
    var subQueryPointer = new SubQueryPointer(subquery, "has_children");
    var hasChildrenJoin =
        SourceVariable.forLeftJoined(subQueryPointer, ConceptAncestor.ANCESTOR_CONCEPT_ID, childId);
    hasChildrenJoin.addJoinClause(
        ConceptAncestor.DESCENDANT_CONCEPT_ID,
        childId,
        BinaryFilterVariable.BinaryOperator.NOT_EQUALS);
    // Ancestors can have children at a minimum of 0 OR 1 levels of separation
    hasChildrenJoin.addJoinClause(
        ConceptAncestor.MIN_LEVELS_OF_SEPARATION,
        new Literal(1),
        BinaryFilterVariable.BinaryOperator.LESS_THAN_OR_EQUAL);
    return hasChildrenJoin;
  }
}
