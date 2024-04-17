package bio.terra.service.snapshotbuilder.utils;


import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
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
import bio.terra.service.snapshotbuilder.utils.constants.ConceptAncestorConstants;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptConstants;
import bio.terra.service.snapshotbuilder.utils.constants.ConceptRelationshipConstants;
import bio.terra.service.snapshotbuilder.utils.constants.ConditionOccurrenceConstants;
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
    TableVariable concept = TableVariable.forPrimary(TablePointer.fromTableName(ConceptConstants.CONCEPT));
    FieldVariable conceptName = concept.makeFieldVariable(ConceptConstants.CONCEPT_NAME);
    FieldVariable conceptId = concept.makeFieldVariable(ConceptConstants.CONCEPT_ID);

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id.
    // We use concept_ancestor for the rollup count because we want to include counts
    // from all descendants, not just direct descendants.
    TableVariable conceptAncestor =
        TableVariable.forJoined(
            TablePointer.fromTableName(ConceptAncestorConstants.CONCEPT_ANCESTOR), ConceptAncestorConstants.ANCESTOR_CONCEPT_ID, conceptId);
    FieldVariable descendantConceptId = conceptAncestor.makeFieldVariable(ConceptAncestorConstants.DESCENDANT_CONCEPT_ID);

    // domain specific occurrence table joined on concept_ancestor.descendant_concept_id =
    // 'domain'_concept_id
    TableVariable domainOccurrence =
        TableVariable.forJoined(
            TablePointer.fromTableName(domainOption.getTableName()),
            domainOption.getColumnName(),
            descendantConceptId);

    // COUNT(DISTINCT person_id)
    FieldVariable count = domainOccurrence.makeFieldVariable(ConditionOccurrenceConstants.PERSON_ID, "COUNT", "count", true);

    List<SelectExpression> select =
        List.of(
            conceptName, conceptId, count, HierarchyQueryBuilder.hasChildrenExpression(concept));

    List<TableVariable> tables = List.of(concept, conceptAncestor, domainOccurrence);

    List<FieldVariable> groupBy = List.of(conceptName, conceptId);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING));

    // WHERE c.concept_id IN ({createSubQuery()}) AND c.standard_concept = 'S'
    FilterVariable where =
        BooleanAndOrFilterVariable.and(
            SubQueryFilterVariable.in(conceptId, createSubQuery(parentConceptId)),
            BinaryFilterVariable.equals(
                concept.makeFieldVariable(ConceptConstants.STANDARD_CONCEPT), new Literal("S")));

    return new Query(select, tables, where, groupBy, orderBy);
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
    TableVariable conceptRelationship =
        TableVariable.forPrimary(TablePointer.fromTableName(ConceptRelationshipConstants.CONCEPT_RELATIONSHIP));
    FieldVariable descendantConceptId = conceptRelationship.makeFieldVariable(ConceptRelationshipConstants.CONCEPT_ID_2);

    return new Query(
        List.of(descendantConceptId),
        List.of(conceptRelationship),
        BooleanAndOrFilterVariable.and(
            BinaryFilterVariable.equals(
                conceptRelationship.makeFieldVariable(ConceptRelationshipConstants.CONCEPT_ID_1), new Literal(conceptId)),
            BinaryFilterVariable.equals(
                conceptRelationship.makeFieldVariable(ConceptRelationshipConstants.RELATIONSHIP_ID), new Literal("Subsumes"))));
  }

  /**
   * Generate a query that retrieves the domain id of the concept specified by the given conceptId.
   *
   * <p>SELECT c.domain_id FROM concept AS c WHERE c.concept_id = conceptId
   */
  public Query retrieveDomainId(int conceptId) {
    TableVariable concept = TableVariable.forPrimary(TablePointer.fromTableName(ConceptConstants.CONCEPT));
    FieldVariable domainIdField = concept.makeFieldVariable(ConceptConstants.DOMAIN_ID);

    BinaryFilterVariable where =
        new BinaryFilterVariable(
            concept.makeFieldVariable(ConceptConstants.CONCEPT_ID),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    List<SelectExpression> select = List.of(domainIdField);
    List<TableVariable> table = List.of(concept);

    return new Query(select, table, where);
  }
}
