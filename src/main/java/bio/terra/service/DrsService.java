package bio.terra.service;

import bio.terra.dao.StudyDao;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.metadata.StudySummary;
import bio.terra.model.DRSObject;
import bio.terra.service.exception.DrsObjectNotFoundException;
import bio.terra.service.exception.InvalidDrsIdException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.UUID;

@Component
public class DrsService {

    private String datarepoDnsName;
    private StudyDao studyDao;
    private FileService fileService;

    @Autowired
    public DrsService(String datarepoDnsName,
                      StudyDao studyDao,
                      FileService fileService) {
        this.datarepoDnsName = datarepoDnsName;
        this.studyDao = studyDao;
        this.fileService = fileService;
    }

    public DRSObject lookupByDrsId(String drsObjectId) {
        DrsId drsId = fromObjectId(drsObjectId);

        UUID studyId = UUID.fromString(drsId.getStudyId());
        try {
            StudySummary study = studyDao.retrieveSummaryById(studyId);
        } catch (StudyNotFoundException ex) {
            throw new DrsObjectNotFoundException("No study found for DRS object id '" + drsObjectId + "'", ex);
        }

        // TODO: validate dataset and check permissions. For temporary testing let that be junk

        DRSObject drsObject = fileService.lookupDrsObject(drsId.getStudyId(), drsId.getFsObjectId());
        return drsObject;
    }

    // -- DrsId parsing and building methods --
    public String toDrsUri(String studyId, String datasetId, String fsObjectId) {
        return DrsId.builder()
            .dnsname(datarepoDnsName)
            .version("v1")
            .studyId(studyId)
            .datasetId(datasetId)
            .fsObjectId(fsObjectId)
            .build()
            .toString();
    }

    public DrsId fromUri(String drsuri) {
        URI uri = URI.create(drsuri);
        if (!StringUtils.equals(uri.getScheme(), "drs") ||
            uri.getAuthority() == null ||
            !StringUtils.startsWith(uri.getPath(), "/")) {
            throw new InvalidDrsIdException("Invalid DRS URI '" + drsuri + "'");
        }

        String objectId = StringUtils.remove(uri.getPath(), '/');
        return parseObjectId(objectId).dnsname(uri.getAuthority()).build();
    }

    public DrsId fromObjectId(String objectId) {
        return parseObjectId(objectId).build();
    }

    private DrsId.Builder parseObjectId(String objectId) {
        // The format is v1_<studyid>_<datasetid>_<fsobjectid>
        String[] idParts = StringUtils.split(objectId, '_');
        if (idParts.length != 4 || !StringUtils.equals(idParts[0], "v1")) {
            throw new InvalidDrsIdException("Invalid DRS object id '" + objectId + "'");
        }

        return DrsId.builder()
            .dnsname(datarepoDnsName)
            .version(idParts[0])
            .studyId(idParts[1])
            .datasetId(idParts[2])
            .fsObjectId(idParts[3]);
    }

}
