package bio.terra.service.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.duos.exception.DuosFirecloudGroupInsertException;
import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DuosService {
  private static final Logger logger = LoggerFactory.getLogger(DuosService.class);

  private final DuosDao duosDao;
  private final IamService iamService;

  @Autowired
  public DuosService(DuosDao duosDao, IamService iamService) {
    this.duosDao = duosDao;
    this.iamService = iamService;
  }

  public Optional<DuosFirecloudGroupModel> retrieveFirecloudGroup(String duosId) {
    return Optional.ofNullable(duosDao.retrieveFirecloudGroup(duosId));
  }

  public DuosFirecloudGroupModel createFirecloudGroup(String duosId) {
    logger.info("Creating Firecloud group for {} users", duosId);

    // First try with the more readable group name.
    String groupName = constructFirecloudGroupName(duosId);
    String groupEmail;
    try {
      groupEmail = iamService.createGroup(groupName);
    } catch (IamConflictException ex) {
      logger.warn(
          "Firecloud group {} already exists: trying creation with a unique name", groupName);
      groupName = constructUniqueFirecloudGroupName(duosId);
      groupEmail = iamService.createGroup(groupName);
    }
    logger.info("Successfully created Firecloud group {} for {} users", groupName, duosId);

    // TODO: when we add a flight to set / unset a snapshot's DUOS ID,
    // this DB insertion should be handled in its own step to allow for
    // teardown of resources created earlier, rather than having to
    // introduce a fallback behavior flow.
    // https://broadworkbench.atlassian.net/browse/DR-2787
    boolean inserted = duosDao.insertFirecloudGroup(duosId, groupName, groupEmail);
    if (!inserted) {
      iamService.deleteGroup(groupName);
      throw new DuosFirecloudGroupInsertException(
          "Firecloud group " + groupName + " was not inserted into the DB and has been deleted");
    }
    return duosDao.retrieveFirecloudGroup(duosId);
  }

  public DuosFirecloudGroupModel retrieveOrCreateFirecloudGroup(String duosId) {
    Optional<DuosFirecloudGroupModel> maybeGroup = retrieveFirecloudGroup(duosId);
    if (maybeGroup.isEmpty()) {
      return createFirecloudGroup(duosId);
    } else {
      logger.info("Firecloud group exists for {}, retrieving.", duosId);
      return maybeGroup.get();
    }
  }

  @VisibleForTesting
  String constructFirecloudGroupName(String duosId) {
    return String.format("%s-users", duosId);
  }

  @VisibleForTesting
  String constructUniqueFirecloudGroupName(String duosId) {
    return String.format("%s-%s", constructFirecloudGroupName(duosId), UUID.randomUUID());
  }
}
