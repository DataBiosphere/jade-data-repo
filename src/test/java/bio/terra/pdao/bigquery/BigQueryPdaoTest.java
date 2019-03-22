package bio.terra.pdao.bigquery;

import bio.terra.category.Connected;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.metadata.Column;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

@RunWith(SpringRunner.class)
@SpringBootTest
@Category(Connected.class)
@ActiveProfiles("bigquery")
public class BigQueryPdaoTest {

    @Autowired
    private BigQueryPdao bigQueryPdao;

    @Test
    public void basicTest() throws Exception {
        // Contrive a study object with a unique name
        String studyName = "pdaotest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
        Study study = makeStudy(studyName);

        boolean exists = bigQueryPdao.studyExists(studyName);
        Assert.assertThat(exists, is(equalTo(false)));

        bigQueryPdao.createStudy(study);

        exists = bigQueryPdao.studyExists(studyName);
        Assert.assertThat(exists, is(equalTo(true)));

        // Perform the redo, which should delete and re-create
        bigQueryPdao.createStudy(study);
        exists = bigQueryPdao.studyExists(studyName);
        Assert.assertThat(exists, is(equalTo(true)));


        // Now delete it and test that it is gone
        bigQueryPdao.deleteStudy(study);
        exists = bigQueryPdao.studyExists(studyName);
        Assert.assertThat(exists, is(equalTo(false)));
    }

    private Study makeStudy(String studyName) {
        Column col1 = new Column().name("col1").type("string");
        Column col2 = new Column().name("col2").type("string");
        Column col3 = new Column().name("col3").type("string");
        Column col4 = new Column().name("col4").type("string");

        List<Column> table1Columns = new ArrayList<>();
        table1Columns.add(col1);
        table1Columns.add(col2);
        Table table1 = new Table().name("table1").columns(table1Columns);

        List<Column> table2Columns = new ArrayList<>();
        table2Columns.add(col4);
        table2Columns.add(col3);
        table2Columns.add(col2);
        table2Columns.add(col1);
        Table table2 = new Table().name("table2").columns(table2Columns);

        List<Table> tables = new ArrayList<>();
        tables.add(table1);
        tables.add(table2);

        Study study = new Study();
        study.name(studyName)
                .description("this is a test study");
        study.tables(tables);
        return study;
    }

}
