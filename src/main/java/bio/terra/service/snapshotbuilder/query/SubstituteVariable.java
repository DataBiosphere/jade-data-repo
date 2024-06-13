package bio.terra.service.snapshotbuilder.query;

public class SubstituteVariable implements SelectExpression {
  private final String searchText;

  public SubstituteVariable(String searchText) {
    this.searchText = searchText;
  }

  // Documentation for @ notation
  // https://cloud.google.com/java/docs/reference/google-cloud-bigquery/latest/com.google.cloud.bigquery.QueryJobConfiguration.Builder#com_google_cloud_bigquery_QueryJobConfiguration_Builder_setNamedParameters_java_util_Map_java_lang_String_com_google_cloud_bigquery_QueryParameterValue__
  // Documentation for : notation
  // https://docs.spring.io/spring-framework/reference/data-access/jdbc/core.html#jdbc-NamedParameterJdbcTemplate
  @Override
  public String renderSQL(SqlRenderContext context) {
    return context.getPlatform().choose("@" + searchText, ":" + searchText);
  }
}
