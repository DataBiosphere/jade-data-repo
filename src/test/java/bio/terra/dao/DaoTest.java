package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Study;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DaoTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StudyDao studyDao;

    private Study study;
    private UUID studyId;
    private Study fromDB;

    @Before
    public void setup() throws Exception {
        if (study == null) {
            ClassLoader classLoader = getClass().getClassLoader();
            String studyJsonStr = IOUtils.toString(classLoader.getResourceAsStream("study-create-test.json"));
            StudyRequestModel studyRequest = objectMapper.readerFor(StudyRequestModel.class).readValue(studyJsonStr);
            studyRequest.setName(studyRequest.getName() + UUID.randomUUID().toString());
            study = StudyJsonConversion.studyRequestToStudy(studyRequest);
            studyId = studyDao.create(study);
            fromDB = studyDao.retrieve(studyId);
        }
    }

    @AfterClass
    public static void teardown() throws Exception {
//        studyDao.delete(study.getId());
    }


    @Test
    public void studyTest() throws Exception {
        assertThat("study name set correctly",
                fromDB.getName(),
                equalTo(study.getName()));
    }

    @Test
    public void assetCreationTest() {
        // verify assets
        assertThat("correct number of assets created",
                fromDB.getAssetSpecifications().size(),
                equalTo(2));
        fromDB.getAssetSpecifications().forEach(this::assertAssetSpecs);
    }

    protected void assertAssetSpecs(AssetSpecification spec) {
        if (spec.getName().equals("Trio")) {
            assertThat("Trio asset has 2 tables",
                    spec.getAssetTables().size(),
                    equalTo(2));
        } else {
            assertThat("other asset created is Sample",
                    spec.getName(),
                    equalTo("Sample"));
        }
    }

}
