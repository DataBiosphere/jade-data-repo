package bio.terra.validation;

import bio.terra.dao.StudyDao;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySpecificationModel;
import bio.terra.model.TableModel;
import bio.terra.pdao.PrimaryDataAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static java.util.stream.Collectors.*;

@Component
public class StudyRequestValidator implements Validator {

    @Autowired
    private PrimaryDataAccess pdao;

    @Autowired
    private StudyDao studyDao;

    @Override
    public boolean supports(Class<?> clazz) {
        return StudyRequestModel.class.equals(clazz);
    }

    private static class SchemaValidationContext {

        HashMap<String, HashSet<String>> tableColumnMap;
        HashSet<String> relationshipNameSet;

        SchemaValidationContext() {
            tableColumnMap = new HashMap<>();
            relationshipNameSet = new HashSet<>();
        }

        void addTable(String tableName, List<String> columnNames) {
            tableColumnMap.put(tableName, new HashSet<>(columnNames));
        }

        void addRelationship(String relationshipName) {
            relationshipNameSet.add(relationshipName);
        }

        boolean isInvalidTable(String tableName) {
            return !tableColumnMap.containsKey(tableName);
        }

        boolean isInvalidTableColumn(String tableName, String columnName) {
            return isInvalidTable(tableName) || !tableColumnMap.get(tableName).contains(columnName);
        }

        boolean isInvalidRelationship(String relationshipName) {
            return !relationshipNameSet.contains(relationshipName);
        }
    }

    private void validateStudyName(String studyName, Errors errors) {
        if (studyName == null) {
            errors.rejectValue("name", "StudyNameMissing");
        }
        else if (!Utils.isValidName(studyName)) {
            errors.rejectValue("name", "StudyNameInvalid");
        }
        else if (pdao.studyExists(studyName) /* || TODO: use studyDao to check existence */) {
            errors.rejectValue("name", "StudyNameInUse");
        }
    }

    private void validateTable(TableModel table, Errors errors, SchemaValidationContext context) {
        String tableName = table.getName();
        List<ColumnModel> columns = table.getColumns();

        if (tableName != null && columns != null) {
            List<String> columnNames = columns.stream().map(ColumnModel::getName).collect(toList());
            if (Utils.hasDuplicates(columnNames)) {
                errors.rejectValue("schema", "DuplicateColumnNames");
            }
            context.addTable(tableName, columnNames);
        }
    }

    private void validateRelationshipTerm(RelationshipTermModel term, Errors errors, SchemaValidationContext context) {
        String table = term.getTable();
        String column = term.getColumn();
        if (table != null && column != null) {
            if (context.isInvalidTableColumn(table, column)) {
                errors.rejectValue("schema", "InvalidRelationshipTermTableColumn");
            }
        }
    }

    private void validateRelationship(RelationshipModel relationship, Errors errors, SchemaValidationContext context) {
        RelationshipTermModel fromTerm = relationship.getFrom();
        if (fromTerm != null) {
            validateRelationshipTerm(fromTerm, errors, context);
        }

        RelationshipTermModel toTerm = relationship.getTo();
        if (toTerm != null) {
            validateRelationshipTerm(toTerm, errors, context);
        }

        String relationshipName = relationship.getName();
        if (relationshipName != null) {
            context.addRelationship(relationshipName);
        }
    }

    private void validateAssetTable(AssetTableModel assetTable, Errors errors, SchemaValidationContext context) {
        String tableName = assetTable.getName();
        List<String> columnNames = assetTable.getColumns();
        if (tableName != null && columnNames != null) {
            // An empty list acts like a wildcard to include all columns from a table in the asset specification.
            if (columnNames.size() == 0) {
                if (context.isInvalidTable(tableName)) {
                    errors.rejectValue("schema", "InvalidAssetTable");
                }
            } else {
                boolean anyInvalidTableColumns = columnNames.stream()
                        .anyMatch((columnName) -> context.isInvalidTableColumn(tableName, columnName));

                if (anyInvalidTableColumns) {
                    errors.rejectValue("schema", "InvalidAssetTableColumn");
                }
            }
        }
    }

    private void validateAsset(AssetModel asset, Errors errors, SchemaValidationContext context) {
        List<AssetTableModel> assetTables = asset.getTables();
        if (assetTables != null) {
            if (assetTables.stream().noneMatch(AssetTableModel::isIsRoot)) {
                errors.rejectValue("schema", "NoRootTable");
            }
            assetTables.forEach((assetTable) -> validateAssetTable(assetTable, errors, context));
        }
        List<String> follows = asset.getFollow();
        if (follows != null) {
            if (follows.stream().anyMatch(context::isInvalidRelationship)) {
                errors.rejectValue("schema", "InvalidFollowsRelationship");
            }
        }
    }

    private void validateSchema(StudySpecificationModel schema, Errors errors) {
        SchemaValidationContext context = new SchemaValidationContext();
        List<TableModel> tables = schema.getTables();
        if (tables != null) {
            List<String> tableNames = tables.stream().map(TableModel::getName).collect(toList());
            if (Utils.hasDuplicates(tableNames)) {
                errors.rejectValue("schema", "DuplicateTableNames");
            }
            tables.forEach((table) -> validateTable(table, errors, context));
        }

        List<RelationshipModel> relationships = schema.getRelationships();
        if (relationships != null) {
            List<String> relationshipNames = relationships.stream().map(RelationshipModel::getName).collect(toList());
            if (Utils.hasDuplicates(relationshipNames)) {
                errors.rejectValue("schema", "DuplicateRelationshipNames");
            }
            relationships.forEach((relationship) -> validateRelationship(relationship, errors, context));
        }

        List<AssetModel> assets = schema.getAssets();
        if (assets != null) {
            List<String> assetNames = assets.stream().map(AssetModel::getName).collect(toList());
            if (Utils.hasDuplicates(assetNames)) {
                errors.rejectValue("schema", "DuplicateAssetNames");
            }
            if (assets.size() == 0) {
                errors.rejectValue("schema", "NoAssets");
            }
            assets.forEach((asset) -> validateAsset(asset, errors, context));
        }
    }

    @Override
    public void validate(Object target, Errors errors) {
        StudyRequestModel studyRequest = (StudyRequestModel) target;
        validateStudyName(studyRequest.getName(), errors);
        StudySpecificationModel schema = studyRequest.getSchema();
        if (schema != null) {
            validateSchema(schema, errors);
        }
    }
}
