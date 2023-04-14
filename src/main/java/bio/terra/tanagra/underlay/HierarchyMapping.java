package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.serialization.UFHierarchyMapping;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class HierarchyMapping {
  // The maximum depth of ancestors present in a hierarchy. This may be larger
  // than the actual max depth, but if it is smaller the resulting table will be incomplete.
  private static final int DEFAULT_MAX_HIERARCHY_DEPTH = 64;

  private static final String ID_FIELD_NAME = "id";
  public static final String CHILD_FIELD_NAME = "child";
  public static final String PARENT_FIELD_NAME = "parent";
  public static final String ANCESTOR_FIELD_NAME = "ancestor";
  public static final String DESCENDANT_FIELD_NAME = "descendant";
  public static final String PATH_FIELD_NAME = "path";
  public static final String NUM_CHILDREN_FIELD_NAME = "num_children";
  public static final String IS_ROOT_FIELD_NAME = "is_root";
  public static final String IS_MEMBER_FIELD_NAME = "is_member";
  private static final AuxiliaryData CHILD_PARENT_AUXILIARY_DATA =
      new AuxiliaryData("childParent", List.of(CHILD_FIELD_NAME, PARENT_FIELD_NAME));
  private static final AuxiliaryData ROOT_NODES_FILTER_AUXILIARY_DATA =
      new AuxiliaryData("rootNodesFilter", List.of(ID_FIELD_NAME));
  private static final AuxiliaryData ANCESTOR_DESCENDANT_AUXILIARY_DATA =
      new AuxiliaryData("ancestorDescendant", List.of(ANCESTOR_FIELD_NAME, DESCENDANT_FIELD_NAME));
  private static final AuxiliaryData PATH_NUM_CHILDREN_AUXILIARY_DATA =
      new AuxiliaryData(
          "pathNumChildren", List.of(ID_FIELD_NAME, PATH_FIELD_NAME, NUM_CHILDREN_FIELD_NAME));

  private final AuxiliaryDataMapping childParent;
  private final AuxiliaryDataMapping rootNodesFilter;
  private final AuxiliaryDataMapping ancestorDescendant;
  private final AuxiliaryDataMapping pathNumChildren;
  private final int maxHierarchyDepth;
  private final Underlay.MappingType mappingType;
  private Hierarchy hierarchy;

  private HierarchyMapping(
      AuxiliaryDataMapping childParent,
      AuxiliaryDataMapping rootNodesFilter,
      AuxiliaryDataMapping ancestorDescendant,
      AuxiliaryDataMapping pathNumChildren,
      int maxHierarchyDepth,
      Underlay.MappingType mappingType) {
    this.childParent = childParent;
    this.rootNodesFilter = rootNodesFilter;
    this.ancestorDescendant = ancestorDescendant;
    this.pathNumChildren = pathNumChildren;
    this.maxHierarchyDepth = maxHierarchyDepth;
    this.mappingType = mappingType;
  }

  public void initialize(Hierarchy hierarchy) {
    this.hierarchy = hierarchy;
  }

  public static HierarchyMapping fromSerialized(
      UFHierarchyMapping serialized, DataPointer dataPointer, Underlay.MappingType mappingType) {
    if (serialized.getChildParent() == null) {
      throw new InvalidConfigException("Child parent pairs are undefined");
    }
    AuxiliaryDataMapping childParent =
        AuxiliaryDataMapping.fromSerialized(
            serialized.getChildParent(), dataPointer, CHILD_PARENT_AUXILIARY_DATA);
    AuxiliaryDataMapping rootNodesFilter =
        serialized.getRootNodesFilter() == null
            ? null
            : AuxiliaryDataMapping.fromSerialized(
                serialized.getRootNodesFilter(), dataPointer, ROOT_NODES_FILTER_AUXILIARY_DATA);
    AuxiliaryDataMapping ancestorDescendant =
        serialized.getAncestorDescendant() == null
            ? null
            : AuxiliaryDataMapping.fromSerialized(
                serialized.getAncestorDescendant(),
                dataPointer,
                ANCESTOR_DESCENDANT_AUXILIARY_DATA);
    AuxiliaryDataMapping pathNumChildren =
        serialized.getPathNumChildren() == null
            ? null
            : AuxiliaryDataMapping.fromSerialized(
                serialized.getPathNumChildren(), dataPointer, PATH_NUM_CHILDREN_AUXILIARY_DATA);
    return new HierarchyMapping(
        childParent,
        rootNodesFilter,
        ancestorDescendant,
        pathNumChildren,
        serialized.getMaxHierarchyDepth(),
        mappingType);
  }

  public static HierarchyMapping defaultIndexMapping(
      String entityName, String hierarchyName, FieldPointer entityIdField, int maxHierarchyDepth) {
    TablePointer entityTable = entityIdField.getTablePointer();
    DataPointer dataPointer = entityTable.getDataPointer();
    String tablePrefix = entityName + "_" + hierarchyName + "_";

    AuxiliaryDataMapping childParent =
        AuxiliaryDataMapping.defaultIndexMapping(
            CHILD_PARENT_AUXILIARY_DATA, tablePrefix, dataPointer);
    AuxiliaryDataMapping ancestorDescendant =
        AuxiliaryDataMapping.defaultIndexMapping(
            ANCESTOR_DESCENDANT_AUXILIARY_DATA, tablePrefix, dataPointer);
    AuxiliaryDataMapping pathNumChildren =
        new AuxiliaryDataMapping(
            entityTable,
            PATH_NUM_CHILDREN_AUXILIARY_DATA.getFields().stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        fieldName -> {
                          String columnName;
                          if (ID_FIELD_NAME.equals(fieldName)) {
                            columnName = entityIdField.getColumnName();
                          } else {
                            columnName =
                                HierarchyField.getFieldAlias(
                                    hierarchyName,
                                    PATH_FIELD_NAME.equals(fieldName)
                                        ? HierarchyField.Type.PATH
                                        : HierarchyField.Type.NUM_CHILDREN);
                          }
                          return new FieldPointer.Builder()
                              .tablePointer(entityTable)
                              .columnName(columnName)
                              .build();
                        })));

    return new HierarchyMapping(
        childParent,
        null,
        ancestorDescendant,
        pathNumChildren,
        maxHierarchyDepth,
        Underlay.MappingType.INDEX);
  }

  public Query queryChildParentPairs(String childFieldAlias, String parentFieldAlias) {
    TableVariable childParentTableVar = TableVariable.forPrimary(childParent.getTablePointer());
    FieldVariable childFieldVar =
        new FieldVariable(
            childParent.getFieldPointers().get(CHILD_FIELD_NAME),
            childParentTableVar,
            childFieldAlias);
    FieldVariable parentFieldVar =
        new FieldVariable(
            childParent.getFieldPointers().get(PARENT_FIELD_NAME),
            childParentTableVar,
            parentFieldAlias);
    return new Query.Builder()
        .select(List.of(childFieldVar, parentFieldVar))
        .tables(List.of(childParentTableVar))
        .build();
  }

  public Query queryPossibleRootNodes(String idFieldAlias) {
    TableVariable possibleRootNodesTableVar =
        TableVariable.forPrimary(rootNodesFilter.getTablePointer());
    FieldVariable idFieldVar =
        new FieldVariable(
            rootNodesFilter.getFieldPointers().get(ID_FIELD_NAME),
            possibleRootNodesTableVar,
            idFieldAlias);
    return new Query.Builder()
        .select(List.of(idFieldVar))
        .tables(List.of(possibleRootNodesTableVar))
        .build();
  }

  public Query queryAncestorDescendantPairs(
      String ancestorFieldAlias, String descendantFieldAlias) {
    TableVariable ancestorDescendantTableVar =
        TableVariable.forPrimary(ancestorDescendant.getTablePointer());
    FieldVariable ancestorFieldVar =
        new FieldVariable(
            ancestorDescendant.getFieldPointers().get(ANCESTOR_FIELD_NAME),
            ancestorDescendantTableVar,
            ancestorFieldAlias);
    FieldVariable descendantFieldVar =
        new FieldVariable(
            ancestorDescendant.getFieldPointers().get(DESCENDANT_FIELD_NAME),
            ancestorDescendantTableVar,
            descendantFieldAlias);
    return new Query.Builder()
        .select(List.of(ancestorFieldVar, descendantFieldVar))
        .tables(List.of(ancestorDescendantTableVar))
        .build();
  }

  public static Query queryPathNumChildrenPairs(TablePointer tablePointer) {
    TableVariable tempTableVar = TableVariable.forPrimary(tablePointer);
    List<TableVariable> inputTables = Lists.newArrayList(tempTableVar);
    FieldVariable selectIdFieldVar =
        new FieldPointer.Builder()
            .tablePointer(tablePointer)
            .columnName(ID_FIELD_NAME)
            .build()
            .buildVariable(tempTableVar, inputTables);
    FieldVariable selectPathFieldVar =
        new FieldPointer.Builder()
            .tablePointer(tablePointer)
            .columnName(PATH_FIELD_NAME)
            .build()
            .buildVariable(tempTableVar, inputTables);
    FieldVariable selectNumChildrenFieldVar =
        new FieldPointer.Builder()
            .tablePointer(tablePointer)
            .columnName(NUM_CHILDREN_FIELD_NAME)
            .build()
            .buildVariable(tempTableVar, inputTables);
    return new Query.Builder()
        .select(List.of(selectIdFieldVar, selectPathFieldVar, selectNumChildrenFieldVar))
        .tables(inputTables)
        .build();
  }

  public FieldPointer getPathField() {
    return buildPathNumChildrenFieldPointerFromEntityId(PATH_FIELD_NAME);
  }

  public FieldPointer getNumChildrenField() {
    return buildPathNumChildrenFieldPointerFromEntityId(NUM_CHILDREN_FIELD_NAME);
  }

  /** Build a field pointer to the PATH or NUM_CHILDREN field, foreign key'd off the entity ID. */
  private FieldPointer buildPathNumChildrenFieldPointerFromEntityId(String fieldName) {
    FieldPointer fieldInAuxTable = getPathNumChildren().getFieldPointers().get(fieldName);
    if (fieldInAuxTable
        .getTablePointer()
        .equals(hierarchy.getEntity().getMapping(mappingType).getTablePointer())) {
      // Field is in the entity table.
      return fieldInAuxTable;
    } else {
      // Field is in a separate table.
      FieldPointer idFieldInAuxTable = pathNumChildren.getFieldPointers().get(ID_FIELD_NAME);
      FieldPointer entityIdFieldPointer =
          hierarchy.getEntity().getIdAttribute().getMapping(mappingType).getValue();

      return new FieldPointer.Builder()
          .tablePointer(entityIdFieldPointer.getTablePointer())
          .columnName(entityIdFieldPointer.getColumnName())
          .foreignTablePointer(pathNumChildren.getTablePointer())
          .foreignKeyColumnName(idFieldInAuxTable.getColumnName())
          .foreignColumnName(fieldInAuxTable.getColumnName())
          .build();
    }
  }

  public AuxiliaryDataMapping getChildParent() {
    return childParent;
  }

  public boolean hasRootNodesFilter() {
    return rootNodesFilter != null;
  }

  public AuxiliaryDataMapping getRootNodesFilter() {
    return rootNodesFilter;
  }

  public boolean hasAncestorDescendant() {
    return ancestorDescendant != null;
  }

  public AuxiliaryDataMapping getAncestorDescendant() {
    return ancestorDescendant;
  }

  public boolean hasPathNumChildren() {
    return pathNumChildren != null;
  }

  public AuxiliaryDataMapping getPathNumChildren() {
    return pathNumChildren;
  }

  public int getMaxHierarchyDepth() {
    return maxHierarchyDepth <= 0 ? DEFAULT_MAX_HIERARCHY_DEPTH : maxHierarchyDepth;
  }
}
