package bio.terra.service.cohortbuilder.tanagra.instances.filter;

import bio.terra.tanagra.exception.InvalidQueryException;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.FunctionFilterVariable;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.AttributeMapping;
import bio.terra.tanagra.underlay.TextSearch;
import bio.terra.tanagra.underlay.TextSearchMapping;
import bio.terra.tanagra.underlay.Underlay;
import java.util.List;

public class TextFilter extends EntityFilter {
  private final TextSearch textSearch;
  private final FunctionFilterVariable.FunctionTemplate functionTemplate;
  private final String text;
  private final Attribute attribute;

  public TextFilter(Builder builder) {
    this.textSearch = builder.textSearch;
    this.functionTemplate = builder.functionTemplate;
    this.text = builder.text;
    this.attribute = builder.attribute;
  }

  @Override
  public FilterVariable getFilterVariable(
      TableVariable entityTableVar, List<TableVariable> tableVars) {
    FieldVariable textSearchFieldVar;
    if (attribute == null) {
      // search against the string defined by the text search mapping
      if (!textSearch.getEntity().getTextSearch().isEnabled()) {
        throw new InvalidQueryException(
            "Text search is not enabled for entity: " + textSearch.getEntity().getName());
      }
      TextSearchMapping textSearchMapping = textSearch.getMapping(Underlay.MappingType.INDEX);
      if (!textSearchMapping.definedBySearchString()) {
        throw new InvalidQueryException(
            "Only text search mapping to a single search string field is currently supported");
      }
      if (functionTemplate.equals(FunctionFilterVariable.FunctionTemplate.TEXT_FUZZY_MATCH)) {
        throw new InvalidQueryException(
            "Only fuzzy matching against a specific attribute is currently supported");
      }
      textSearchFieldVar =
          textSearchMapping.getSearchString().buildVariable(entityTableVar, tableVars);
    } else {
      // search against a specific attribute
      AttributeMapping attributeMapping = attribute.getMapping(Underlay.MappingType.INDEX);
      if (attribute.getType().equals(Attribute.Type.KEY_AND_DISPLAY)) {
        // use the display field
        textSearchFieldVar = attributeMapping.getDisplay().buildVariable(entityTableVar, tableVars);
      } else {
        // use the value field
        textSearchFieldVar = attributeMapping.getValue().buildVariable(entityTableVar, tableVars);
      }
    }
    return new FunctionFilterVariable(functionTemplate, textSearchFieldVar, new Literal(text));
  }

  public static class Builder {
    private TextSearch textSearch;
    private FunctionFilterVariable.FunctionTemplate functionTemplate;
    private String text;
    private Attribute attribute;

    public Builder textSearch(TextSearch textSearch) {
      this.textSearch = textSearch;
      return this;
    }

    public Builder functionTemplate(FunctionFilterVariable.FunctionTemplate functionTemplate) {
      this.functionTemplate = functionTemplate;
      return this;
    }

    public Builder text(String text) {
      this.text = text;
      return this;
    }

    public Builder attribute(Attribute attribute) {
      this.attribute = attribute;
      return this;
    }

    public TextFilter build() {
      return new TextFilter(this);
    }
  }
}
