package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class ConceptChildrenQueryBuilder {

  private ConceptChildrenQueryBuilder() {}

  private static final String CONCEPT = "concept";
  private static final String CONCEPT_ANCESTOR = "concept_ancestor";
  private static final String PERSON_ID = "person_id";
  private static final String DOMAIN_ID = "domain_id";
  private static final String CONCEPT_ID = "concept_id";
  private static final String CONCEPT_NAME = "concept_name";
  private static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  private static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";

  /**
   * Generate a query that retrieves the children of the given concept and their roll-up counts.
   *
   * <p>SELECT cc.concept_id, cc.concept_name, COUNT(DISTINCT co.person_id) as count FROM `concept`
   * AS cc JOIN `concept_ancestor` AS ca ON cc.concept_id = ca.ancestor_concept_id JOIN
   * `'domain'_occurrence` AS co ON co.'domain'_concept_id = ca.descendant_concept_id WHERE
   * (c.concept_id IN (SELECT c.descendant_concept_id FROM `concept_ancestor` AS c WHERE
   * c.ancestor_concept_id = conceptId) AND c.concept_id != conceptId) GROUP BY cc.concept_id,
   * cc.concept_name ORDER BY cc.concept_name ASC
   */
  public static String buildConceptChildrenQuery(
      SnapshotBuilderDomainOption domainOption,
      int conceptId,
      TableNameGenerator tableNameGenerator,
      CloudPlatformWrapper platform) {

    // concept table and its fields concept_name and concept_id
    var conceptTablePointer = TablePointer.fromTableName(CONCEPT, tableNameGenerator);
    var conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    var nameFieldVariable = conceptTableVariable.makeFieldVariable(CONCEPT_NAME);
    var idFieldVariable = conceptTableVariable.makeFieldVariable(CONCEPT_ID);

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id
    var conceptAncestorTablePointer =
        TablePointer.fromTableName(CONCEPT_ANCESTOR, tableNameGenerator);
    var conceptAncestorTableVariable =
        TableVariable.forJoined(conceptAncestorTablePointer, ANCESTOR_CONCEPT_ID, idFieldVariable);
    var descendantIdFieldVariable =
        conceptAncestorTableVariable.makeFieldVariable(DESCENDANT_CONCEPT_ID);

    // domain specific occurrence table joined on concept_ancestor.descendant_concept_id =
    // 'domain'_concept_id
    var domainOccurrenceTablePointer =
        TablePointer.fromTableName(domainOption.getTableName(), tableNameGenerator);
    var domainOccurenceTableVariable =
        TableVariable.forJoined(
            domainOccurrenceTablePointer, domainOption.getColumnName(), descendantIdFieldVariable);

    // COUNT(DISTINCT person_id)
    var countFieldVariable =
        new FieldVariable(
            new FieldPointer(domainOccurrenceTablePointer, PERSON_ID, "COUNT"),
            domainOccurenceTableVariable,
            "count",
            true);

    List<SelectExpression> select = List.of(nameFieldVariable, idFieldVariable, countFieldVariable);

    List<TableVariable> tables =
        List.of(conceptTableVariable, conceptAncestorTableVariable, domainOccurenceTableVariable);

    List<FieldVariable> groupBy = List.of(nameFieldVariable, idFieldVariable);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(nameFieldVariable, OrderByDirection.ASCENDING));

    // ancestorTable is primary table for the subquery
    var ancestorTablePointer = TablePointer.fromTableName(CONCEPT_ANCESTOR, tableNameGenerator);
    var ancestorTableVariable = TableVariable.forPrimary(ancestorTablePointer);
    var descendantFieldVariable = ancestorTableVariable.makeFieldVariable(DESCENDANT_CONCEPT_ID);

    // WHERE c.ancestor_concept_id = conceptId
    BinaryFilterVariable ancestorClause =
        new BinaryFilterVariable(
            ancestorTableVariable.makeFieldVariable(ANCESTOR_CONCEPT_ID),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    // WHERE d.descendant_concept_id != conceptId
    BinaryFilterVariable notSelfClause =
        new BinaryFilterVariable(
            ancestorTableVariable.makeFieldVariable(DESCENDANT_CONCEPT_ID),
            BinaryFilterVariable.BinaryOperator.NOT_EQUALS,
            new Literal(conceptId));

    // WHERE c.ancestor_concept_id = conceptId AND d.descendant_concept_id != conceptId
    List<FilterVariable> clauses = List.of(ancestorClause, notSelfClause);
    BooleanAndOrFilterVariable subqueryWhereClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.AND, clauses);

    // (SELECT c.descendant_concept_id FROM concept_ancestor AS c
    // WHERE c.ancestor_concept_id = conceptId)
    Query subQuery =
        new Query(
            List.of(descendantFieldVariable), List.of(ancestorTableVariable), subqueryWhereClause);

    // WHERE c.concept_id IN subQuery
    SubQueryFilterVariable mainWhereClause =
        new SubQueryFilterVariable(idFieldVariable, SubQueryFilterVariable.Operator.IN, subQuery);

    Query query = new Query(select, tables, mainWhereClause, groupBy, orderBy);
    return query.renderSQL(platform);
  }

  public static String retrieveDomainId(
      Integer conceptId, TableNameGenerator tableNameGenerator, CloudPlatformWrapper platform) {
    TablePointer conceptTablePointer = TablePointer.fromTableName(CONCEPT, tableNameGenerator);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldVariable domainIdFieldVariable = conceptTableVariable.makeFieldVariable(DOMAIN_ID);
    BinaryFilterVariable whereClause =
        new BinaryFilterVariable(
            conceptTableVariable.makeFieldVariable(CONCEPT_ID),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    Query query =
        new Query(List.of(domainIdFieldVariable), List.of(conceptTableVariable), whereClause);
    return query.renderSQL(platform);
  }
}
