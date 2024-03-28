package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class ConceptChildrenQueryBuilder {

  private ConceptChildrenQueryBuilder() {}

  public static String buildConceptChildrenQuery(
      int conceptId, TableNameGenerator tableNameGenerator, CloudPlatformWrapper platform) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", tableNameGenerator);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldVariable nameFieldVariable = conceptTableVariable.makeFieldVariable("concept_name");
    FieldVariable idFieldVariable = conceptTableVariable.makeFieldVariable("concept_id");

    TablePointer relationshipTablePointer =
        TablePointer.fromTableName("concept_relationship", tableNameGenerator);
    TableVariable relationshipTableVariable = TableVariable.forPrimary(relationshipTablePointer);
    FieldVariable descendantFieldVariable =
        relationshipTableVariable.makeFieldVariable("concept_id_2");

    BooleanAndOrFilterVariable whereClause =
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.AND,
            List.of(
                new BinaryFilterVariable(
                    relationshipTableVariable.makeFieldVariable("concept_id_1"),
                    BinaryFilterVariable.BinaryOperator.EQUALS,
                    new Literal(conceptId)),
                new BinaryFilterVariable(
                    relationshipTableVariable.makeFieldVariable("relationship_id"),
                    BinaryFilterVariable.BinaryOperator.EQUALS,
                    new Literal("Subsumes"))));
    Query subQuery =
        new Query(
            List.of(descendantFieldVariable), List.of(relationshipTableVariable), whereClause);
    FilterVariable filterVariable =
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.AND,
            List.of(
                new SubQueryFilterVariable(
                    idFieldVariable, SubQueryFilterVariable.Operator.IN, subQuery),
                new BinaryFilterVariable(
                    conceptTableVariable.makeFieldVariable("standard_concept"),
                    BinaryFilterVariable.BinaryOperator.EQUALS,
                    new Literal("S"))));
    Query query =
        new Query(
            List.of(nameFieldVariable, idFieldVariable),
            List.of(conceptTableVariable),
            filterVariable);
    return query.renderSQL(platform);
  }
}
