package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.ProfileFixtures;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudySummary;
import bio.terra.metadata.Table;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.resourcemanagement.dao.ProfileDao;
import org.hamcrest.Matchers;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class StudyDaoTest {

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private StudyDao studyDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private Study study;
    private UUID studyId;
    private Study fromDB;
    private BillingProfile billingProfile;
    private boolean deleted;

    @Before
    public void setup() throws Exception {
        billingProfile = ProfileFixtures.randomBillingProfile();
        UUID profileId = profileDao.createBillingProfile(billingProfile);
        billingProfile.id(profileId);

        StudyRequestModel studyRequest = jsonLoader.loadObject("study-create-test.json",
            StudyRequestModel.class);
        studyRequest
            .name(studyRequest.getName() + UUID.randomUUID().toString())
            .defaultProfileId(profileId.toString());
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
        studyId = studyDao.create(study);
        fromDB = studyDao.retrieve(studyId);
    }

    @After
    public void teardown() throws Exception {
        if (!deleted) {
            studyDao.delete(studyId);
        }
        studyId = null;
        fromDB = null;
        profileDao.deleteBillingProfileById(billingProfile.getId());
    }

    private UUID createMinimalStudy() throws IOException {
        StudyRequestModel studyRequest = jsonLoader.loadObject("study-minimal.json",
            StudyRequestModel.class);
        studyRequest
            .name(studyRequest.getName() + UUID.randomUUID().toString())
            .defaultProfileId(billingProfile.getId().toString());
        return studyDao.create(StudyJsonConversion.studyRequestToStudy(studyRequest));
    }

    @Test(expected = StudyNotFoundException.class)
    public void studyDeleteTest() {
        boolean success = studyDao.delete(studyId);
        deleted = success;
        studyDao.retrieve(studyId);
    }

    @Test
    public void enumerateTest() throws Exception {
        UUID study1 = createMinimalStudy();
        List<UUID> studyIds = new ArrayList<>();
        studyIds.add(study1);
        studyIds.add(studyId);

        MetadataEnumeration<StudySummary> summaryEnum = studyDao.enumerate(0, 2, "created_date",
            "asc", null, studyIds);
        List<StudySummary> studies = summaryEnum.getItems();
        assertThat("study enumerate limit param works",
            studies.size(),
            equalTo(2));

        assertThat("study enumerate returns studies in the order created",
            studies.get(0).getCreatedDate().toEpochMilli(),
                Matchers.lessThan(studies.get(1).getCreatedDate().toEpochMilli()));

        // this is skipping the first item returned above
        // so compare the id from the previous retrieve
        assertThat("study enumerate offset param works",
            studyDao.enumerate(1, 1, "created_date", "asc", null, studyIds)
                .getItems().get(0).getId(),
            equalTo(studies.get(1).getId()));

        studyDao.delete(study1);
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
                    equalTo(rel.getFromColumn().getTable().getId()));
            UUID toUUID = jdbcTemplate.queryForObject(sqlTo, params, UUID.class);
            assertThat("to table id in DB matches that in retrieved object",
                    toUUID,
                    equalTo(rel.getToColumn().getTable().getId()));
        });
    }

    protected void assertStudyTable(Table table) {
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
                    spec.getRootTable().getTable().getName(),
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
                    equalTo("sample"));
            assertThat("Sample asset has 2 tables",
                    spec.getAssetTables().size(),
                    equalTo(2));
            assertThat("sample is the root table",
                    spec.getRootTable().getTable().getName(),
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
