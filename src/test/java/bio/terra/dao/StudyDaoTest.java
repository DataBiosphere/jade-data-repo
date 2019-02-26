package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTable;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class StudyDaoTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StudyDao studyDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private Study study;
    private UUID studyId;
    private Study fromDB;
    private boolean deleted;

    @Before
    public void setup() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        String studyJsonStr = IOUtils.toString(classLoader.getResourceAsStream("study-create-test.json"));
        StudyRequestModel studyRequest = objectMapper.readerFor(StudyRequestModel.class).readValue(studyJsonStr);
        studyRequest.setName(studyRequest.getName() + UUID.randomUUID().toString());
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
        studyId = studyDao.create(study);
        fromDB = studyDao.retrieve(studyId);
    }

    @After
    public void teardown() throws Exception {
        if (!deleted) {
            studyDao.delete(studyId);
        }
    }


    @Test
    public void studyTest() throws Exception {
        assertThat("study name is set correctly",
                fromDB.getName(),
                equalTo(study.getName()));

        // verify tables
        assertThat("correct number of tables created for study",
                fromDB.getTables().size(),
                equalTo(2));
        fromDB.getTables().forEach(this::assertStudyTable);

        assertThat("correct number of relationships are created for study",
                fromDB.getRelationships().size(),
                equalTo(2));

        assertTablesInRelationship(fromDB);

        // verify assets
        assertThat("correct number of assets created for study",
                fromDB.getAssetSpecifications().size(),
                equalTo(2));
        fromDB.getAssetSpecifications().forEach(this::assertAssetSpecs);
    }

    protected void assertTablesInRelationship(Study study) {
        String sqlFrom = "SELECT from_table "
                + "FROM study_relationship WHERE id = :id";
        String sqlTo = "SELECT to_table "
                + "FROM study_relationship WHERE id = :id";
        study.getRelationships().stream().forEach(rel -> {
            MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", rel.getId());
            UUID fromUUID = jdbcTemplate.queryForObject(sqlFrom, params, UUID.class);
            assertThat("from table id in DB matches that in retrieved object",
                    fromUUID,
                    equalTo(rel.getFromColumn().getInTable().getId()));
            UUID toUUID = jdbcTemplate.queryForObject(sqlTo, params, UUID.class);
            assertThat("to table id in DB matches that in retrieved object",
                    toUUID,
                    equalTo(rel.getToColumn().getInTable().getId()));
        });
    }

    protected void assertStudyTable(StudyTable table) {
        if (table.getName().equals("participant")) {
            assertThat("participant table has 4 columns",
                    table.getColumns().size(),
                    equalTo(4));
        } else {
            assertThat("other table created is sample",
                    table.getName(),
                    equalTo("sample"));
            assertThat("sample table has 3 columns",
                    table.getColumns().size(),
                    equalTo(3));
        }

    }

    protected void assertAssetSpecs(AssetSpecification spec) {
        if (spec.getName().equals("Trio")) {
            assertThat("Trio asset has 2 tables",
                    spec.getAssetTables().size(),
                    equalTo(2));
            assertThat("participant is the root table for Trio",
                    spec.getRootTable().getStudyTable().getName(),
                    equalTo("participant"));
            assertThat("participant asset table has only 3 columns",
                    spec.getRootTable().getColumns().size(),
                    equalTo(3));
            assertThat("Trio asset follows 2 relationships",
                    spec.getAssetRelationships().size(),
                    equalTo(2));
        } else {
            assertThat("other asset created is Sample",
                    spec.getName(),
                    equalTo("Sample"));
            assertThat("Sample asset has 2 tables",
                    spec.getAssetTables().size(),
                    equalTo(2));
            assertThat("sample is the root table",
                    spec.getRootTable().getStudyTable().getName(),
                    equalTo("sample"));
            assertThat("and 3 columns",
                    spec.getRootTable().getColumns().size(),
                    equalTo(3));
            assertThat("Sample asset follows 1 relationship",
                    spec.getAssetRelationships().size(),
                    equalTo(1));
        }
    }
}
