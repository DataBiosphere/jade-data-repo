package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.SqlPlatform;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;

public class SubQueryFilterVariable extends FilterVariable {
  private static final String SUBSTITUTION_TEMPLATE = "${fieldVariable} ${operator} (${subQuery})";

  private final FieldVariable fieldVariable;
  private final Operator operator;
  private final Query subQuery;

  public SubQueryFilterVariable(FieldVariable fieldVariable, Operator operator, Query subQuery) {
    this.fieldVariable = fieldVariable;
    this.operator = operator;
    this.subQuery = subQuery;
  }

  @Override
  protected String getSubstitutionTemplate(SqlPlatform platform) {
    Map<String, String> params =
        ImmutableMap.<String, String>builder()
            .put("operator", operator.renderSQL(platform))
            .put("subQuery", subQuery.renderSQL(platform))
            .build();
    return StringSubstitutor.replace(SUBSTITUTION_TEMPLATE, params);
  }

  @Override
  public List<FieldVariable> getFieldVariables() {
    return List.of(fieldVariable);
  }

  public enum Operator implements SQLExpression {
    IN("IN"),
    NOT_IN("NOT IN");

    private String sql;

    Operator(String sql) {
      this.sql = sql;
    }

    @Override
    public String renderSQL(SqlPlatform platform) {
      return sql;
    }
  }
}
