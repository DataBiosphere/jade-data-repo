package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class ConceptChildrenQueryBuilder {

  private ConceptChildrenQueryBuilder() {}

  public static String buildConceptChildrenQuery(
      SnapshotBuilderDomainOption domainOption,
      int conceptId,
      TableNameGenerator tableNameGenerator,
      CloudPlatformWrapper platform) {

    // concept table and its fields concept_name and concept_id
    var conceptTablePointer = TablePointer.fromTableName("concept", tableNameGenerator);
    var conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    var nameFieldVariable = conceptTableVariable.makeFieldVariable("concept_name");
    var idFieldVariable = conceptTableVariable.makeFieldVariable("concept_id");

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id
    var conceptAncestorTablePointer =
        TablePointer.fromTableName("concept_ancestor", tableNameGenerator);
    var conceptAncestorTableVariable =
        TableVariable.forJoined(
            conceptAncestorTablePointer, "ancestor_concept_id", idFieldVariable);
    var descendantIdFieldVariable =
        conceptAncestorTableVariable.makeFieldVariable("descendant_concept_id");

    // domain specific occurrence table joined on  concept.concept_id = 'domain'_concept_id
    var domainOccurrenceTablePointer =
        TablePointer.fromTableName(domainOption.getTableName(), tableNameGenerator);
    var domainOccurenceTableVariable =
        TableVariable.forJoined(
            domainOccurrenceTablePointer, domainOption.getColumnName(), descendantIdFieldVariable);

    // COUNT(DISTINCT person_id)
    var countFieldVariable =
        new FieldVariable(
            new FieldPointer(
                domainOccurrenceTablePointer, CriteriaQueryBuilder.PERSON_ID_FIELD_NAME, "COUNT"),
            domainOccurenceTableVariable,
            "count",
            true);

    List<FieldVariable> select = List.of(nameFieldVariable, idFieldVariable, countFieldVariable);

    List<TableVariable> tables =
        List.of(conceptTableVariable, conceptAncestorTableVariable, domainOccurenceTableVariable);

    List<FieldVariable> groupBy = List.of(nameFieldVariable, idFieldVariable);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(nameFieldVariable, OrderByDirection.ASCENDING));

    // ancestorTable is primary table for the subquery
    var ancestorTablePointer = TablePointer.fromTableName("concept_ancestor", tableNameGenerator);
    var ancestorTableVariable = TableVariable.forPrimary(ancestorTablePointer);
    var descendantFieldVariable = ancestorTableVariable.makeFieldVariable("descendant_concept_id");

    // WHERE c.ancestor_concept_id = conceptId
    BinaryFilterVariable whereClause =
        new BinaryFilterVariable(
            ancestorTableVariable.makeFieldVariable("ancestor_concept_id"),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    // (SELECT c.descendant_concept_id FROM concept_ancestor AS c
    // WHERE c.ancestor_concept_id = conceptId)
    Query subQuery =
        new Query(List.of(descendantFieldVariable), List.of(ancestorTableVariable), whereClause);

    // WHERE c.concept_id IN subQuery
    SubQueryFilterVariable subQueryFilterVariable =
        new SubQueryFilterVariable(idFieldVariable, SubQueryFilterVariable.Operator.IN, subQuery);

    // SELECT cc.concept_id, cc.concept_name, COUNT(DISTINCT co.person_id) as count
    // FROM `concept` AS cc
    // JOIN `concept_ancestor` AS ca
    // ON  cc.concept_id = ca.ancestor_concept_id
    // JOIN `'domain'_occurrence` AS co
    // ON co.'domain'_concept_id = ca.descendant_concept_id
    // WHERE ca.ancestor_concept_id IN
    // (SELECT c.descendant_concept_id FROM `concept_ancestor` AS c WHERE c.ancestor_concept_id =
    // conceptId)
    // GROUP BY cc.concept_id, cc.concept_name
    // ORDER BY cc.concept_name ASC
    Query query = new Query(select, tables, subQueryFilterVariable, groupBy, orderBy);
    return query.renderSQL(platform);
  }

  public static String retrieveDomainId(
      Integer conceptId, TableNameGenerator tableNameGenerator, CloudPlatformWrapper platform) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", tableNameGenerator);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldVariable domainIdFieldVariable = conceptTableVariable.makeFieldVariable("domain_id");
    BinaryFilterVariable whereClause =
        new BinaryFilterVariable(
            conceptTableVariable.makeFieldVariable("concept_id"),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    Query query =
        new Query(List.of(domainIdFieldVariable), List.of(conceptTableVariable), whereClause);
    return query.renderSQL(platform);
  }
}
