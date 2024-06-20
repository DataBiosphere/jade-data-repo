package bio.terra.service.auth.iam;

public class FirecloudGroupModel {
  private String groupName;
  private String groupEmail;

  public FirecloudGroupModel groupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  public FirecloudGroupModel groupEmail(String groupEmail) {
    this.groupEmail = groupEmail;
    return this;
  }

  public String getGroupName() {
    return groupName;
  }

  public String getGroupEmail() {
    return groupEmail;
  }
}
