package bio.terra.grammar;


import bio.terra.common.category.Unit;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.grammar.exception.MissingDatasetException;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@Category(Unit.class)
public class GrammarTest {

    private Map<String, DatasetModel> datasetMap;

    @Before
    public void setup() {
        DatasetModel datasetModel = new DatasetModel().dataProject("a-data-project");
        datasetMap = Collections.singletonMap("dataset", datasetModel);
    }


    @Test
    public void testDatasetNames() {
        Query query = Query.parse("SELECT * FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
        List<String> datasetNames = query.getDatasetNames();
        assertThat("there are two datasets", datasetNames.size(), equalTo(2));
        assertThat("it found the right datasets", datasetNames, hasItems("foo", "baz"));
    }


    @Test
    public void testTableNames() {
        Query query = Query.parse("SELECT foo.bar.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
        List<String> tableNames = query.getTableNames();
        assertThat("there are three columns", tableNames.size(), equalTo(2));
        assertThat("it found the right tables", tableNames, hasItems("bar", "quux"));
    }

    @Test
    public void testColumnNames() {
        Query query = Query.parse("SELECT foo.bar.datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
        List<String> columnNames = query.getColumnNames();
        assertThat("there are three columns", columnNames.size(), equalTo(3));
        assertThat("it found the right columns", columnNames, hasItems("datarepo_row_id", "x", "y"));
    }

    @Test(expected = InvalidQueryException.class)
    public void testColumnNotFullyQualifiedName() {
        // note that the col `datarepo_row_id` does not have a table or dataset attached
        Query.parse("SELECT datarepo_row_id FROM foo.bar, baz.quux WHERE foo.bar.x = baz.quux.y");
    }

    @Test
    public void testBqTranslate() {
        BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
        Query query = Query.parse("SELECT * FROM dataset.table WHERE dataset.table.x = 'string'");
        String translated = query.translateSql(bqVisitor);
        assertThat("query translates to valid bigquery syntax", translated,
            equalTo("SELECT * FROM `a-data-project.dataset.table` WHERE `a-data-project.dataset.table.x` = 'string'"));
    }

    // test missing key

    @Test
    public void testAliasedColumn() {
        //Query query = Query.parse("SELECT * FROM dataset.table A WHERE A.col = 'foo'");
        //BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
        // String translated = query.translateSql(bqVisitor);
        // TODO the rest of this test
    }

    @Test(expected = InvalidQueryException.class)
    public void testInvalidQuery() {
        Query query = Query.parse("this is not a valid query");
        query.getDatasetNames();
    }

    @Test(expected = MissingDatasetException.class)
    public void testInvalidDataset() {
        BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
        Query query = Query.parse("SELECT * FROM noDataset.table WHERE noDataset.table.x = 'string'");
        query.translateSql(bqVisitor);
    }

}
