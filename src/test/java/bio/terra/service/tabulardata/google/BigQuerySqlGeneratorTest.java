package bio.terra.service.tabulardata.google;

import bio.terra.common.Column;
import bio.terra.common.Table;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.DatasetTable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.terra.common.PdaoConstant.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@Category(Unit.class)
public class BigQuerySqlGeneratorTest {

    private static final String bqProject = "bq_project";
    private static final Dataset dataset = new Dataset(
        new DatasetSummary()
            .id(UUID.randomUUID())
            .name("dataset_name"));
    private static final String projectDataset =
        bqProject + "." + prefixName(dataset.getName());
    private static final String stagingName = "staging_table_1234";

    @Test
    public void testAddRowIdsToStagingTable () {
        String generated = BigQuerySqlGenerator.addRowIdsToStagingTable(
            bqProject, dataset, stagingName);
        String expected = "UPDATE `" + projectDataset + ".staging_table_1234` SET " +
            PDAO_ROW_ID_COLUMN + " = GENERATE_UUID() WHERE " +
            PDAO_ROW_ID_COLUMN + " IS NULL";

        assertThat(generated, equalTo(expected));
    }

    @Test
    public void testInsertIntoDatasetTable() {
        List<Column> columns = Stream.of("column_one", "column_2", "a_third_column")
            .map(name -> new Column().name(name))
            .collect(Collectors.toList());
        Table table = new DatasetTable().name("data_table").columns(columns);

        String generated = BigQuerySqlGenerator.insertIntoDatasetTable(
            bqProject, dataset, table, stagingName);
        String expected = "INSERT `" + projectDataset + ".data_table` (" +
            PDAO_ROW_ID_COLUMN + ",column_one,column_2,a_third_column) SELECT " +
            PDAO_ROW_ID_COLUMN + ",column_one,column_2,a_third_column FROM `" +
            projectDataset + ".staging_table_1234`";

        assertThat(generated, equalTo(expected));
    }
}
