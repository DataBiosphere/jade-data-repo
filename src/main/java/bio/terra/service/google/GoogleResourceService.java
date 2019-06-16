package bio.terra.service.google;

import bio.terra.dao.exception.google.ProjectNotFoundException;
import bio.terra.dao.google.GoogleResourceDao;
import bio.terra.flight.exception.InaccessibleBillingAccountException;
import bio.terra.metadata.google.GoogleProject;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.ProfileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class GoogleResourceService {
    private Logger logger = LoggerFactory.getLogger(GoogleResourceService.class);

    private final GoogleResourceDao resourceDao;
    private final ProfileService profileService;

    @Autowired
    public GoogleResourceService(GoogleResourceDao resourceDao, ProfileService profileService) {
        this.resourceDao = resourceDao;
        this.profileService = profileService;
    }

    public GoogleProject getProject(UUID studyId, UUID profileId) {
        try {
            return resourceDao.retrieveProjectBy("profile_id", profileId);
        } catch (ProjectNotFoundException e) {
            logger.info("Creating project since none found for profile: " + profileId);
        }
        return newProject(studyId, profileId);
    }

    public GoogleProject newProject(UUID studyId, UUID profileId) {
        // look up the profile
        BillingProfileModel profile = profileService.getProfileById(profileId);
        if (!profile.isAccessible()) {
            throw new InaccessibleBillingAccountException("The repository needs access to this billing account");
        }
        // TODO: pull in project creation code
    }
}
