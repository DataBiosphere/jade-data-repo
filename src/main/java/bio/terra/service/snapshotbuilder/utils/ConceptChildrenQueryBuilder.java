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

    // FROM concept JOIN domainOccurrenceTablePointer ON domainOccurrenceTablePointer.'concept_id' =
    // concept.concept_id
    var domainOccurrenceTablePointer =
        TablePointer.fromTableName(domainOption.getTableName(), tableNameGenerator);
    var domainOccurenceTableVariable =
        TableVariable.forJoined(
            domainOccurrenceTablePointer, domainOption.getColumnName(), idFieldVariable);

    // COUNT(DISTINCT person_id)
    var countFieldVariable =
        new FieldVariable(
            new FieldPointer(
                domainOccurrenceTablePointer, CriteriaQueryBuilder.PERSON_ID_FIELD_NAME, "COUNT"),
            domainOccurenceTableVariable,
            "count",
            true);

    List<FieldVariable> select = List.of(nameFieldVariable, idFieldVariable, countFieldVariable);

    List<TableVariable> tables = List.of(conceptTableVariable, domainOccurenceTableVariable);

    List<FieldVariable> groupBy = List.of(nameFieldVariable, idFieldVariable);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(nameFieldVariable, OrderByDirection.ASCENDING));

    // ancestorTable is primary table for the subquery
    var ancestorTablePointer = TablePointer.fromTableName("concept_ancestor", tableNameGenerator);
    var ancestorTableVariable = TableVariable.forPrimary(ancestorTablePointer);
    var descendantFieldVariable = ancestorTableVariable.makeFieldVariable("descendant_concept_id");

    // WHERE c.ancestor_concept_id = conceptId)
    BinaryFilterVariable whereClause =
        new BinaryFilterVariable(
            ancestorTableVariable.makeFieldVariable("ancestor_concept_id"),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    // (SELECT c.descendant_concept_id FROM concept_ancestor AS c
    // WHERE c.ancestor_concept_id = conceptId)
    Query subQuery =
        new Query(List.of(descendantFieldVariable), List.of(ancestorTableVariable), whereClause);

    // WHERE c.concept_id IN
    //       (SELECT c.descendant_concept_id FROM concept_ancestor AS c
    //       WHERE c.ancestor_concept_id = conceptId)
    SubQueryFilterVariable subQueryFilterVariable =
        new SubQueryFilterVariable(idFieldVariable, SubQueryFilterVariable.Operator.IN, subQuery);

    // SELECT c.concept_name, c.concept_id, COUNT(DISTINCT c0.person_id) AS count
    // FROM concept AS c
    // JOIN condition_occurrence AS c0
    // ON c0.condition_concept_id = c.concept_id
    // WHERE c.concept_id IN
    //    (SELECT c.descendant_concept_id FROM concept_ancestor AS c
    //     WHERE c.ancestor_concept_id = 101)
    // GROUP BY c.concept_name, c.concept_id
    // ORDER BY c.concept_name ASC
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
