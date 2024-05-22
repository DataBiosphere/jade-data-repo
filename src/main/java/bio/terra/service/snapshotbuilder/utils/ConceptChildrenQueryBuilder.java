package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
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
import bio.terra.service.snapshotbuilder.query.tables.Table;
import java.util.List;

public class ConceptChildrenQueryBuilder {
  /**
   * Generate a query that retrieves the descendants of the given concept and their roll-up counts.
   *
   * <p>SELECT cc.concept_id, cc.concept_name, COUNT(DISTINCT co.person_id) as count FROM `concept`
   * AS cc JOIN `concept_ancestor` AS ca ON cc.concept_id = ca.ancestor_concept_id JOIN
   * `'domain'_occurrence` AS co ON co.'domain'_concept_id = ca.descendant_concept_id WHERE
   * (c.concept_id IN (SELECT c.concept_id_2 FROM concept_relationship AS c WHERE (c.concept_id_1 =
   * 101 AND * c.relationship_id = 'Subsumes')) AND c.standard_concept = 'S') GROUP BY
   * cc.concept_id, cc.concept_name ORDER BY cc.concept_name ASC
   */
  public Query buildConceptChildrenQuery(
      SnapshotBuilderDomainOption domainOption, int parentConceptId) {

    // concept table and its fields concept_name and concept_id
    Concept concept = Concept.asPrimary();
    FieldVariable conceptName = concept.name();
    FieldVariable conceptId = concept.concept_id();
    FieldVariable conceptCode = concept.code();

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id.
    // We use concept_ancestor for the rollup count because we want to include counts
    // from all descendants, not just direct descendants.
    ConceptAncestor conceptAncestor = ConceptAncestor.joinAncestor(conceptId);

    // domain specific occurrence table joined on concept_ancestor.descendant_concept_id =
    // 'domain'_concept_id
    DomainOccurrence domainOccurrence = DomainOccurrence.leftJoinOnDescendantConcept(domainOption);

    // COUNT(DISTINCT person_id)
    FieldVariable countPerson = domainOccurrence.getCountPerson();

    List<SelectExpression> select =
        List.of(
            conceptName,
            conceptId,
            conceptCode,
            countPerson,
            HierarchyQueryBuilder.hasChildrenExpression(conceptId));

    List<Table> tables = List.of(concept, conceptAncestor, domainOccurrence);

    List<FieldVariable> groupBy = List.of(conceptName, conceptId, conceptCode);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING));

    // WHERE c.concept_id IN ({createSubQuery()}) AND c.standard_concept = 'S'
    FilterVariable where =
        BooleanAndOrFilterVariable.and(
            SubQueryFilterVariable.in(conceptId, createSubQuery(parentConceptId)),
            BinaryFilterVariable.equals(concept.standardConcept(), new Literal("S")));

    return new Query.Builder()
        .select(select)
        .tables(tables)
        .where(where)
        .groupBy(groupBy)
        .orderBy(orderBy)
        .build();
  }

  /**
   * Generate a query that retrieves the descendants of the given concept. We use concept
   * relationship here because it includes only the direct descendants.
   *
   * <p>SELECT c.concept_id_2 FROM concept_relationship AS c WHERE (c.concept_id_1 = 101 AND
   * c.relationship_id = 'Subsumes'))
   */
  Query createSubQuery(int conceptId) {
    // concept_relationship is primary table for the subquery
    ConceptRelationship conceptRelationship = ConceptRelationship.asPrimary();
    FieldVariable descendantConceptId = conceptRelationship.concept_id_2();

    return new Query.Builder()
        .select(List.of(descendantConceptId))
        .tables(List.of(conceptRelationship))
        .where(
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(
                    conceptRelationship.concept_id_1(), new Literal(conceptId)),
                BinaryFilterVariable.equals(
                    conceptRelationship.relationship_id(), new Literal("Subsumes"))))
        .build();
  }

  /**
   * Generate a query that retrieves the domain id of the concept specified by the given conceptId.
   *
   * <p>SELECT c.domain_id FROM concept AS c WHERE c.concept_id = conceptId
   */
  public Query retrieveDomainId(int conceptId) {
    Concept concept = Concept.asPrimary();
    FieldVariable domainId = concept.domain_id();

    BinaryFilterVariable where =
        new BinaryFilterVariable(
            concept.concept_id(),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    return new Query.Builder()
        .select(List.of(domainId))
        .tables(List.of(concept))
        .where(where)
        .build();
  }
}
