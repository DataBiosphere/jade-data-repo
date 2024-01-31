package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.QueryBuilderUtils.makeFieldVariable;

import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import java.util.List;

public class SearchConceptsQueryBuilder {

  private SearchConceptsQueryBuilder() {}

  public static String buildSearchConceptsQuery(
      String domainId, String searchText, TableNameGenerator tableNameGenerator) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", tableNameGenerator);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldVariable nameFieldVariable =
        makeFieldVariable(conceptTablePointer, conceptTableVariable, "concept_name");
    FieldVariable idFieldVariable =
        makeFieldVariable(conceptTablePointer, conceptTableVariable, "concept_id");

    // domain clause has field name domain_id
    BinaryFilterVariable domainClause =
        createDomainClause(conceptTablePointer, conceptTableVariable, domainId);

    // searchConceptName clause has field name "concept_name"
    FunctionFilterVariable searchConceptNameClause =
        createSearchConceptClause(
            conceptTablePointer, conceptTableVariable, searchText, "concept_name");

    // searchConceptCode clause has field name "concept_code"
    FunctionFilterVariable searchConceptCodeClause =
        createSearchConceptClause(
            conceptTablePointer, conceptTableVariable, searchText, "concept_code");

    // DomainClause AND (SearchConceptNameClause OR searchConceptCodeClause)
    List<FilterVariable> searches = List.of(searchConceptNameClause, searchConceptCodeClause);

    BooleanAndOrFilterVariable searchClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.OR, searches);
    List<FilterVariable> allFilters = List.of(domainClause, searchClause);

    BooleanAndOrFilterVariable whereClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.AND, allFilters);

    Query query =
        new Query(
            List.of(nameFieldVariable, idFieldVariable),
            List.of(conceptTableVariable),
            whereClause);

    return query.renderSQL();
  }

  static FunctionFilterVariable createSearchConceptClause(
      TablePointer conceptTablePointer,
      TableVariable conceptTableVariable,
      String searchText,
      String columnName) {
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
        new FieldVariable(new FieldPointer(conceptTablePointer, columnName), conceptTableVariable),
        new Literal(searchText));
  }

  static BinaryFilterVariable createDomainClause(
      TablePointer conceptTablePointer, TableVariable conceptTableVariable, String domainId) {
    return new BinaryFilterVariable(
        new FieldVariable(new FieldPointer(conceptTablePointer, "domain_id"), conceptTableVariable),
        BinaryFilterVariable.BinaryOperator.EQUALS,
        new Literal(domainId));
  }
}
