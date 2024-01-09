package bio.terra.tanagra.underlay;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.UnionQuery;
import bio.terra.tanagra.serialization.UFTextSearchMapping;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class TextSearchMapping {
  public static final String TEXT_SEARCH_ID_COLUMN_NAME = "id";
  public static final String TEXT_SEARCH_STRING_COLUMN_NAME = "text";
  private static final AuxiliaryData TEXT_SEARCH_STRING_AUXILIARY_DATA =
      new AuxiliaryData(
          "textsearch", List.of(TEXT_SEARCH_ID_COLUMN_NAME, TEXT_SEARCH_STRING_COLUMN_NAME));

  private final List<Attribute> attributes;
  private final FieldPointer searchString;
  private final AuxiliaryDataMapping searchStringTable;
  private final Underlay.MappingType mappingType;
  private TextSearch textSearch;

  private TextSearchMapping(Builder builder) {
    this.attributes = builder.attributes;
    this.searchString = builder.searchString;
    this.searchStringTable = builder.searchStringTable;
    this.mappingType = builder.mappingType;
  }

  public void initialize(TextSearch textSearch) {
    this.textSearch = textSearch;
  }

  public static TextSearchMapping fromSerialized(
      UFTextSearchMapping serialized,
      TablePointer tablePointer,
      Map<String, Attribute> entityAttributes,
      Underlay.MappingType mappingType) {
    if (serialized.getAttributes() != null && serialized.getSearchString() != null) {
      throw new InvalidConfigException(
          "Text search mapping can be defined by either attributes or a search string, not both");
    }

    if (serialized.getAttributes() != null) {
      if (serialized.getAttributes().isEmpty()) {
        throw new InvalidConfigException("Text search mapping list of attributes is empty");
      }
      List<Attribute> attributesForTextSearch =
          serialized.getAttributes().stream()
              .map(entityAttributes::get)
              .collect(Collectors.toList());
      return new Builder().attributes(attributesForTextSearch).mappingType(mappingType).build();
    }

    if (serialized.getSearchString() != null) {
      FieldPointer searchStringField =
          FieldPointer.fromSerialized(serialized.getSearchString(), tablePointer);
      return new Builder().searchString(searchStringField).mappingType(mappingType).build();
    }

    if (serialized.getSearchStringTable() != null) {
      AuxiliaryDataMapping searchStringTable =
          AuxiliaryDataMapping.fromSerialized(
              serialized.getSearchStringTable(),
              tablePointer.getDataPointer(),
              TEXT_SEARCH_STRING_AUXILIARY_DATA);
      return new Builder().searchStringTable(searchStringTable).mappingType(mappingType).build();
    }

    throw new InvalidConfigException("Text search mapping is empty");
  }

  public static TextSearchMapping defaultIndexMapping(TablePointer tablePointer) {
    return new Builder()
        .searchString(
            new FieldPointer.Builder()
                .tablePointer(tablePointer)
                .columnName(TEXT_SEARCH_STRING_COLUMN_NAME)
                .build())
        .mappingType(Underlay.MappingType.INDEX)
        .build();
  }

  public Query queryTextSearchStrings(QueryExecutor executor) {
    SQLExpression idAllTextPairs;
    FieldPointer entityIdField =
        textSearch.getEntity().getIdAttribute().getMapping(mappingType).getValue();
    if (definedByAttributes()) {
      idAllTextPairs =
          new UnionQuery(
              getAttributes().stream()
                  .map(
                      attribute -> {
                        FieldPointer textField;
                        if (Attribute.Type.SIMPLE == attribute.getType()) {
                          textField = attribute.getMapping(mappingType).getValue();
                          if (Literal.DataType.STRING != attribute.getDataType()) {
                            textField =
                                textField.toBuilder()
                                    // FIXME: was STRING in BQ, MS SQL wants VARCHAR
                                    .sqlFunctionWrapper("CAST(${fieldSql} AS VARCHAR)")
                                    .build();
                          }
                        } else {
                          textField = attribute.getMapping(mappingType).getDisplay();
                        }
                        return buildIdTextPairsQuery(entityIdField, textField);
                      })
                  .collect(Collectors.toList()));
    } else if (definedBySearchString()) {
      idAllTextPairs = buildIdTextPairsQuery(entityIdField, getSearchString());
    } else if (definedBySearchStringAuxiliaryData()) {
      idAllTextPairs =
          buildIdTextPairsQuery(
              searchStringTable.getFieldPointers().get(TEXT_SEARCH_ID_COLUMN_NAME),
              searchStringTable.getFieldPointers().get(TEXT_SEARCH_STRING_COLUMN_NAME));
    } else {
      throw new SystemException("Unknown text search mapping type");
    }

    TablePointer idTextPairsTable =
        TablePointer.fromRawSql(
            executor.renderSQL(idAllTextPairs),
            textSearch.getEntity().getMapping(mappingType).getTablePointer().getDataPointer());
    FieldPointer idField =
        new FieldPointer.Builder()
            .tablePointer(idTextPairsTable)
            .columnName(TEXT_SEARCH_ID_COLUMN_NAME)
            .build();
    FieldPointer concatenatedTextField =
        new FieldPointer.Builder()
            .tablePointer(idTextPairsTable)
            .columnName(TEXT_SEARCH_STRING_COLUMN_NAME)
            .sqlFunctionWrapper("STRING_AGG(${fieldSql}, ',')")
            .build();

    TableVariable idTextPairsTableVar = TableVariable.forPrimary(idTextPairsTable);
    FieldVariable idFieldVar =
        new FieldVariable(idField, idTextPairsTableVar, TEXT_SEARCH_ID_COLUMN_NAME);
    FieldVariable concatenatedTextFieldVar =
        new FieldVariable(
            concatenatedTextField, idTextPairsTableVar, TEXT_SEARCH_STRING_COLUMN_NAME);
    return new Query.Builder()
        .select(List.of(idFieldVar, concatenatedTextFieldVar))
        .tables(List.of(idTextPairsTableVar))
        .groupBy(List.of(idFieldVar))
        .build();
  }

  private Query buildIdTextPairsQuery(FieldPointer entityIdField, FieldPointer textField) {
    TableVariable entityTableVar = TableVariable.forPrimary(entityIdField.getTablePointer());
    List<TableVariable> tableVars = new ArrayList<>();
    tableVars.add(entityTableVar);

    FieldVariable textFieldVar =
        textField.buildVariable(entityTableVar, tableVars, TEXT_SEARCH_STRING_COLUMN_NAME);
    FieldVariable entityIdFieldVar =
        entityIdField.buildVariable(entityTableVar, tableVars, TEXT_SEARCH_ID_COLUMN_NAME);
    return new Query.Builder()
        .select(List.of(entityIdFieldVar, textFieldVar))
        .tables(tableVars)
        .build();
  }

  public TablePointer getTablePointer() {
    if (definedByAttributes()) {
      return textSearch.getEntity().getMapping(mappingType).getTablePointer();
    } else if (definedBySearchString()) {
      return searchString.getTablePointer();
    } else if (definedBySearchStringAuxiliaryData()) {
      return searchStringTable.getTablePointer();
    } else {
      throw new SystemException("Unknown text search mapping type");
    }
  }

  public boolean definedByAttributes() {
    return attributes != null;
  }

  public boolean definedBySearchString() {
    return searchString != null;
  }

  public boolean definedBySearchStringAuxiliaryData() {
    return searchStringTable != null;
  }

  public List<Attribute> getAttributes() {
    return Collections.unmodifiableList(attributes);
  }

  public FieldPointer getSearchString() {
    return searchString;
  }

  public AuxiliaryDataMapping getSearchStringTable() {
    return searchStringTable;
  }

  public ColumnSchema buildTextColumnSchema() {
    return new ColumnSchema(TEXT_SEARCH_STRING_COLUMN_NAME, CellValue.SQLDataType.STRING);
  }

  public static class Builder {
    private List<Attribute> attributes;
    private FieldPointer searchString;
    private AuxiliaryDataMapping searchStringTable;
    private Underlay.MappingType mappingType;

    public Builder attributes(List<Attribute> attributes) {
      this.attributes = attributes;
      return this;
    }

    public Builder searchString(FieldPointer searchString) {
      this.searchString = searchString;
      return this;
    }

    public Builder searchStringTable(AuxiliaryDataMapping searchStringTable) {
      this.searchStringTable = searchStringTable;
      return this;
    }

    public Builder mappingType(Underlay.MappingType mappingType) {
      this.mappingType = mappingType;
      return this;
    }

    public TextSearchMapping build() {
      return new TextSearchMapping(this);
    }
  }
}
