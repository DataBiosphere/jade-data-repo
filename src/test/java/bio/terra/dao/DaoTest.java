package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Study;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DaoTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StudyDao studyDao;

    Study study;

    @Before
    public void setup() throws  Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        String studyJsonStr = IOUtils.toString(classLoader.getResourceAsStream("study-create-test.json"));
        StudyRequestModel studyRequest = objectMapper.readerFor(StudyRequestModel.class).readValue(studyJsonStr);
        studyRequest.setName(studyRequest.getName()+UUID.randomUUID().toString());
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
    }

    @After
    public void teardown() throws Exception {
//        studyDao.delete(study.getId());
    }


    @Test
    public void basicTest() throws Exception {
        UUID studyId = studyDao.create(study);
        Study fromDB = studyDao.retrieve(studyId);
        assertThat("UUID returned equal UUID of study",
                fromDB.getId(),
                equalTo(studyId));
//                study.getAssetSpecifications().values()
//                        .stream()
//                        .map(spec -> spec.getId())
//                        .collect(Collectors.toList()),
//                containsInAnyOrder(assetIds.toArray()));
//        assetIds.forEach(assetId -> {
//            AssetSpecification spec = assetDao.retrieveAssetSpecification(assetId);
//        });
    }

}
