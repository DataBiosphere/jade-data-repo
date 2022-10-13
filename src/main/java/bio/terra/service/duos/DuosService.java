package bio.terra.service.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

@Component
public class DuosService {
  private static final Logger logger = LoggerFactory.getLogger(DuosService.class);

  private final DuosClient duosClient;
  private final DuosDao duosDao;
  private final IamService iamService;

  @Autowired
  public DuosService(DuosClient duosClient, DuosDao duosDao, IamService iamService) {
    this.duosClient = duosClient;
    this.duosDao = duosDao;
    this.iamService = iamService;
  }

  public DuosFirecloudGroupModel getOrCreateFirecloudGroup(String duosId) {
    Optional<DuosFirecloudGroupModel> maybeGroup = duosDao.retrieveDuosFirecloudGroup(duosId);

    DuosFirecloudGroupModel group;
    if (maybeGroup.isEmpty()) {
      logger.info("Creating Firecloud group for {} users...", duosId);
      String groupName = constructFirecloudGroupName(duosId);
      try {
        iamService.createGroup(groupName);
      } catch (HttpClientErrorException ex) {
        if (ex.getStatusCode() == HttpStatus.CONFLICT) {
          logger.warn("Firecloud group " + groupName + " already exists", ex);
          groupName = constructUniqueFirecloudGroupName(duosId);
          iamService.createGroup(groupName);
        }
        throw ex;
      }
      String groupEmail = iamService.getGroupEmail(groupName);
      logger.info("Created Firecloud group for {} users: {}", duosId, groupName);

      var maybeInsertedGroup =
          duosDao.insertFirecloudGroupAndRetrieve(duosId, groupName, groupEmail);
      if (maybeInsertedGroup.isPresent()) {
        group = maybeInsertedGroup.get();
        supplementGroupAdmin(group, "okotsopo.broad.test@gmail.com");
      } else {
        throw new RuntimeException("Firecloud group was not inserted into DB");
      }
    } else {
      group = maybeGroup.get();
      // TODO actually try to get the group, does it exist?
      logger.info(
          "{} already had Firecloud group with name {}", duosId, group.getFirecloudGroupName());
    }
    return group;
  }

  // TODO for demo only... ultimately we will remove.
  private void supplementGroupAdmin(DuosFirecloudGroupModel firecloudGroup, String email) {
    String groupName = firecloudGroup.getFirecloudGroupName();
    try {
      logger.info("Trying to add {} as Firecloud group {} admin", email, groupName);
      iamService.addEmailToGroup(groupName, IamRole.ADMIN.toString(), email);
      logger.info("Successfully added {} as Firecloud group {} admin", email, groupName);
    } catch (Exception ex) {
      // Hmm, seems to throw a 404 if the email is already a member.
      logger.error(
          "Error trying to add " + email + " as Firecloud Group " + groupName + " admin!", ex);
    }
  }

  private String constructFirecloudGroupName(String duosId) {
    return String.format("%s-users", duosId);
  }

  private String constructUniqueFirecloudGroupName(String duosId) {
    return String.format("%s-%s", constructFirecloudGroupName(duosId), UUID.randomUUID());
  }
}
