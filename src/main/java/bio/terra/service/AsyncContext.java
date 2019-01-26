package bio.terra.service;

import bio.terra.dao.StudyDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AsyncContext {

    @Autowired
    private StudyDAO studyDAO;

    public StudyDAO getStudyDAO() {
        return studyDAO;
    }

}
