package bio.terra.service.dataproject;

import bio.terra.dao.exception.DataProjectNotFoundException;
import bio.terra.resourcemanagement.dao.google.GoogleResourceNotFoundException;
import bio.terra.dao.DataProjectDao;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.Study;
import bio.terra.metadata.DatasetDataProject;
import bio.terra.metadata.DatasetDataProjectSummary;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectRequest;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;
import bio.terra.metadata.StudyDataProject;
import bio.terra.metadata.StudyDataProjectSummary;
import bio.terra.resourcemanagement.service.google.GoogleResourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Component
public class DataProjectService {

    private static final Logger logger = LoggerFactory.getLogger(DataProjectService.class);
    private static final List<String> DATA_PROJECT_SERVICE_IDS =  Arrays.asList(
        "bigquery-json.googleapis.com",
        "firestore.googleapis.com",
        "firebaserules.googleapis.com",
        "storage-component.googleapis.com",
        "storage-api.googleapis.com"
    );

    private final DataProjectDao dataProjectDao;
    private final DataProjectIdSelector dataProjectIdSelector;
    private final GoogleResourceService resourceService;

    @Autowired
    public DataProjectService(
            DataProjectDao dataProjectDao,
            DataProjectIdSelector dataProjectIdSelector,
            GoogleResourceService resourceService) {
        this.dataProjectDao = dataProjectDao;
        this.dataProjectIdSelector = dataProjectIdSelector;
        this.resourceService = resourceService;
    }

    public DatasetDataProject getProjectForDataset(Dataset dataset) {
        DatasetDataProjectSummary datasetDataProjectSummary = null;
        GoogleProjectResource googleProjectResource;
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataProjectIdSelector.projectIdForDataset(dataset))
            .profileId(dataset.getProfile().getId())
            .serviceIds(DATA_PROJECT_SERVICE_IDS);
        try {
            datasetDataProjectSummary = dataProjectDao.retrieveDatasetDataProjectByDatasetId(dataset.getId());
            googleProjectResource = resourceService.getProjectResourceById(
                datasetDataProjectSummary.getProjectResourceId());
        } catch (DataProjectNotFoundException | GoogleResourceNotFoundException e) {
            // probably the first time we have seen this dataset, request a new project resource and save everything
            googleProjectResource = resourceService.getOrCreateProject(googleProjectRequest);
            if (datasetDataProjectSummary != null) {
                logger.warn("metadata has a project resource id it can't resolve for dataset: " + dataset.getName());
                dataProjectDao.deleteDatasetDataProject(datasetDataProjectSummary.getId());
            }
            datasetDataProjectSummary = new DatasetDataProjectSummary()
                .projectResourceId(googleProjectResource.getRepositoryId())
                .datasetId(dataset.getId());
            UUID datasetDataProjectId = dataProjectDao.createDatasetDataProject(datasetDataProjectSummary);
            datasetDataProjectSummary.id(datasetDataProjectId);
        }
        return new DatasetDataProject(datasetDataProjectSummary)
            .googleProjectResource(googleProjectResource);
    }

    public StudyDataProject getProjectForStudy(Study study) {
        StudyDataProjectSummary studyDataProjectSummary = null;
        GoogleProjectResource googleProjectResource;
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataProjectIdSelector.projectIdForStudy(study))
            .profileId(study.getDefaultProfileId())
            .serviceIds(DATA_PROJECT_SERVICE_IDS);
        try {
            studyDataProjectSummary = dataProjectDao.retrieveStudyDataProjectByStudyId(study.getId());
            googleProjectResource = resourceService.getProjectResourceById(
                studyDataProjectSummary.getProjectResourceId());
        } catch (DataProjectNotFoundException | GoogleResourceNotFoundException e) {
            // probably the first time we have seen this study, request a new project resource and save everything
            googleProjectResource = resourceService.getOrCreateProject(googleProjectRequest);
            if (studyDataProjectSummary != null) {
                logger.warn("metadata has a project resource id it can't resolve for study: " + study.getName());
                dataProjectDao.deleteStudyDataProject(studyDataProjectSummary.getId());
            }
            studyDataProjectSummary = new StudyDataProjectSummary()
                .projectResourceId(googleProjectResource.getRepositoryId())
                .studyId(study.getId());
            UUID studyDataProjectId = dataProjectDao.createStudyDataProject(studyDataProjectSummary);
            studyDataProjectSummary.id(studyDataProjectId);
        }
        return new StudyDataProject(studyDataProjectSummary)
            .googleProjectResource(googleProjectResource);
    }
}
