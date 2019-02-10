package bio.terra.flight.study.create;

import bio.terra.category.Unit;
import bio.terra.metadata.Study;
import bio.terra.model.*;
import bio.terra.pdao.PrimaryDataAccess;
import bio.terra.stairway.*;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest
@Category(Unit.class)
public class StudyCreateFlightTest {

    @Autowired
    private Stairway stairway;

    @Autowired
    PrimaryDataAccess pdao;

    private String studyName;
    private StudyRequestModel studyRequest;
    private Study study;

    @Before
    public void setup() {
        studyName = "scftest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
        List<RelationshipModel> relationships = Arrays.asList(
                new RelationshipModel()
                        .name("participant_sample")
                        .from(new RelationshipTermModel()
                                .table("participant")
                                .column("id")
                                .cardinality(RelationshipTermModel.CardinalityEnum.ONE))
                        .to(new RelationshipTermModel()
                                .table("sample")
                                .column("participant_id")
                                .cardinality(RelationshipTermModel.CardinalityEnum.MANY)));
        List<TableModel> studyTables = Arrays.asList(
                new TableModel()
                        .name("participant")
                        .columns(Arrays.asList(
                                new ColumnModel().name("id").datatype("string"),
                                new ColumnModel().name("age").datatype("integer"))),
                new TableModel()
                        .name("sample")
                        .columns(Arrays.asList(
                                new ColumnModel().name("id").datatype("string"),
                                new ColumnModel().name("participant_id").datatype("string"),
                                new ColumnModel().name("date_collected").datatype("date"))));
        List<AssetModel> assets = Arrays.asList(
                new AssetModel()
                        .name("Sample")
                        .tables(Arrays.asList(
                                new AssetTableModel()
                                        .name("sample")
                                        .isRoot(true),
                                new AssetTableModel()
                                        .name("participant")))
                        .follow(Arrays.asList("participant_sample")));
        studyRequest = new StudyRequestModel()
                .name(studyName)
                .description("This is a study definition used in StudyCreateFlightTest.")
                .schema(new StudySpecificationModel()
                        .tables(studyTables)
                        .relationships(relationships)
                        .assets(assets));
        study = new Study(studyRequest);
    }

    @After
    public void tearDown() {
        // TODO: cleanup study using the DAO if it still exists

        if (pdao.studyExists(studyName)) {
            pdao.deleteStudy(study);
        }
    }

    @Test
    public void testHappyPath() {
        FlightMap map = new FlightMap();
        map.put("request", studyRequest);
        String flightId = stairway.submit(StudyCreateFlight.class, map);
        FlightResult result = stairway.getResult(flightId);
        Assert.assertTrue(result.isSuccess());
        // TODO: check that the DAO can read the study
        Assert.assertTrue(pdao.studyExists(studyName));
    }

    @Test
    public void testUndoAfterPrimaryDataStep() {
        FlightMap map = new FlightMap();
        map.put("request", studyRequest);
        String flightId = stairway.submit(UndoStudyCreateFlight.class, map);
        FlightResult result = stairway.getResult(flightId);
        Assert.assertFalse(result.isSuccess());
        Optional<Throwable> optionalThrowable = result.getThrowable();
        Assert.assertTrue(optionalThrowable.isPresent());
        Assert.assertEquals(optionalThrowable.get().getMessage(), "TestTriggerUndoStep");
        // TODO: use the DAO to make sure the study is cleaned up
        Assert.assertFalse(pdao.studyExists(studyName));
    }
}
