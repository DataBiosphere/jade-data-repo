package bio.terra.pdao.bigquery;

import bio.terra.category.Connected;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
        StudyTableColumn col1 = new StudyTableColumn("col1", "string");
        StudyTableColumn col2 = new StudyTableColumn("col2", "string");
        StudyTableColumn col3 = new StudyTableColumn("col3", "string");
        StudyTableColumn col4 = new StudyTableColumn("col4", "string");

        Map<String, StudyTableColumn> tableMap1 = new HashMap<>();
        tableMap1.put(col1.getName(), col1);
        tableMap1.put(col2.getName(), col2);
        StudyTable table1 = new StudyTable("table1", tableMap1);

        Map<String, StudyTableColumn> tableMap2 = new HashMap<>();
        tableMap2.put(col4.getName(), col4);
        tableMap2.put(col3.getName(), col3);
        tableMap2.put(col2.getName(), col2);
        tableMap2.put(col1.getName(), col1);
        StudyTable table2 = new StudyTable("table2", tableMap2);

        Map<String, StudyTable> tables = new HashMap<>();
        tables.put(table1.getName(), table1);
        tables.put(table2.getName(), table2);

        Study study = new Study(studyName,
                "this is a test study",
                tables,
                Collections.EMPTY_MAP,
                Collections.EMPTY_MAP);
        return study;
    }

}
