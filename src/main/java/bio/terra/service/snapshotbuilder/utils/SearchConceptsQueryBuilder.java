package bio.terra.service.snapshotbuilder.utils;

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

  public static String buildSearchConceptsQuery(
      String domainId, String searchText, TableNameGenerator tableNameGenerator) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", tableNameGenerator);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldPointer nameFieldPointer = new FieldPointer(conceptTablePointer, "concept_name");
    FieldVariable nameFieldVariable = new FieldVariable(nameFieldPointer, conceptTableVariable);
    FieldPointer idFieldPointer = new FieldPointer(conceptTablePointer, "concept_id");
    FieldVariable idFieldVariable = new FieldVariable(idFieldPointer, conceptTableVariable);

    BinaryFilterVariable domainClause =
        new BinaryFilterVariable(
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "domain_id"), conceptTableVariable),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(domainId));
    FunctionFilterVariable searchConceptNameClause =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "concept_name"), conceptTableVariable),
            new Literal(searchText));
    FunctionFilterVariable searchConceptCodeClause =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "concept_code"), conceptTableVariable),
            new Literal(searchText));
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
}
