package bio.terra.service.tabulardata.google.bigquery;

import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
@Profile("google")
public class BigQueryCohortBuilderPDao {
  public TableResult getInstanceCounts(String entityId, List<String> attributes)
      throws InterruptedException {
    BigQueryProject project = BigQueryProject.get("");
    String sql =
        """
        SELECT COUNT(<idField>) AS idField, <attributes> FROM `<project>.<dataset>`.<table> AS p GROUP BY <attributes> ORDER BY <attributesAsc>
        """;
    ST sqlTemplate = new ST(sql);
    sqlTemplate.add("project", "broad-bio.terra.tanagra-dev");
    sqlTemplate.add("dataset", "cmssynpuf_index_011523");
    sqlTemplate.add("table", entityId);
    sqlTemplate.add("idField", "id");
    sqlTemplate.add("attributes", String.join(",", attributes));
    sqlTemplate.add(
        "attributes",
        attributes.stream()
            .map(attribute -> attribute.concat(" ASC"))
            .collect(Collectors.joining(",")));
    return project.query(sqlTemplate.render());
  }
}
