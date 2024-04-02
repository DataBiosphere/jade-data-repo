package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
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
    TableVariable conceptTable =
        TableVariable.forPrimary(TablePointer.fromTableName(CONCEPT, tableNameGenerator));
    FieldVariable nameField = conceptTable.makeFieldVariable(CONCEPT_NAME);
    FieldVariable idField = conceptTable.makeFieldVariable(CONCEPT_ID);

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id
    TableVariable conceptAncestorTable =
        TableVariable.forJoined(
            TablePointer.fromTableName(CONCEPT_ANCESTOR, tableNameGenerator),
            ANCESTOR_CONCEPT_ID,
            idField);
    FieldVariable descendantIdField = conceptAncestorTable.makeFieldVariable(DESCENDANT_CONCEPT_ID);

    // domain specific occurrence table joined on concept_ancestor.descendant_concept_id =
    // 'domain'_concept_id
    TablePointer domainOccurrenceTablePointer =
        TablePointer.fromTableName(domainOption.getTableName(), tableNameGenerator);
    TableVariable domainOccurrenceTable =
        TableVariable.forJoined(
            domainOccurrenceTablePointer, domainOption.getColumnName(), descendantIdField);

    // COUNT(DISTINCT person_id)
    FieldVariable countFieldVariable =
        domainOccurrenceTable.makeFieldVariable(PERSON_ID, "COUNT", "count", true);

    List<SelectExpression> select = List.of(nameField, idField, countFieldVariable);

    List<TableVariable> tables = List.of(conceptTable, conceptAncestorTable, domainOccurrenceTable);

    List<FieldVariable> groupBy = List.of(nameField, idField);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(nameField, OrderByDirection.ASCENDING));

    Query subQuery = createSubQuery(conceptId, tableNameGenerator);

    // WHERE c.concept_id IN subQuery
    SubQueryFilterVariable where = SubQueryFilterVariable.in(idField, subQuery);

    Query query = new Query(select, tables, where, groupBy, orderBy);
    return query.renderSQL(platform);
  }

  static Query createSubQuery(int conceptId, TableNameGenerator tableNameGenerator) {
    // ancestorTable is primary table for the subquery
    TableVariable ancestorTable =
        TableVariable.forPrimary(TablePointer.fromTableName(CONCEPT_ANCESTOR, tableNameGenerator));
    FieldVariable descendantField = ancestorTable.makeFieldVariable(DESCENDANT_CONCEPT_ID);

    // WHERE c.ancestor_concept_id = conceptId
    BinaryFilterVariable ancestorClause =
        BinaryFilterVariable.equals(
            ancestorTable.makeFieldVariable(ANCESTOR_CONCEPT_ID), new Literal(conceptId));

    // WHERE d.descendant_concept_id != conceptId
    BinaryFilterVariable notSelfClause =
        BinaryFilterVariable.notEquals(
            ancestorTable.makeFieldVariable(DESCENDANT_CONCEPT_ID), new Literal(conceptId));

    // WHERE c.ancestor_concept_id = conceptId AND d.descendant_concept_id != conceptId
    BooleanAndOrFilterVariable subqueryWhere =
        BooleanAndOrFilterVariable.and(ancestorClause, notSelfClause);

    // (SELECT c.descendant_concept_id FROM concept_ancestor AS c
    // WHERE c.ancestor_concept_id = conceptId)
    return new Query(List.of(descendantField), List.of(ancestorTable), subqueryWhere);
  }

  /**
   * Generate a query that retrieves the domain id of the concept specified by the given conceptId.
   *
   * <p>SELECT c.domain_id FROM concept AS c WHERE c.concept_id = conceptId
   */
  public static String retrieveDomainId(
      int conceptId, TableNameGenerator tableNameGenerator, CloudPlatformWrapper platform) {
    TableVariable concept =
        TableVariable.forPrimary(TablePointer.fromTableName(CONCEPT, tableNameGenerator));
    FieldVariable domainIdField = concept.makeFieldVariable(DOMAIN_ID);

    BinaryFilterVariable where =
        new BinaryFilterVariable(
            concept.makeFieldVariable(CONCEPT_ID),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    List<SelectExpression> select = List.of(domainIdField);
    List<TableVariable> table = List.of(concept);

    Query query = new Query(select, table, where);
    return query.renderSQL(platform);
  }
}
