package bio.terra.service.journal;

import bio.terra.model.IamResourceTypeEnum;
import bio.terra.model.JournalEntryModel;
import bio.terra.service.auth.iam.IamResourceType;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class JournalEntry {
  private UUID id;
  private JournalService.EntryType entryType;
  private String user;
  private UUID resourceKey;
  private IamResourceType resourceType;
  private String className;
  private String methodName;
  private Map mutations;

  private String note;
  private Instant when;

  public UUID getId() {
    return id;
  }

  public JournalEntry id(UUID id) {
    this.id = id;
    return this;
  }

  public JournalService.EntryType getEntryType() {
    return entryType;
  }

  public JournalEntry entryType(JournalService.EntryType entryType) {
    this.entryType = entryType;
    return this;
  }

  public String getUser() {
    return user;
  }

  public JournalEntry user(String user) {
    this.user = user;
    return this;
  }

  public UUID getResourceKey() {
    return resourceKey;
  }

  public JournalEntry resourceKey(UUID key) {
    this.resourceKey = key;
    return this;
  }

  public IamResourceType getResourceType() {
    return resourceType;
  }

  public JournalEntry resourceType(IamResourceType resourceType) {
    this.resourceType = resourceType;
    return this;
  }

  public String getClassName() {
    return className;
  }

  public JournalEntry className(String className) {
    this.className = className;
    return this;
  }

  public String getMethodName() {
    return methodName;
  }

  public JournalEntry methodName(String methodName) {
    this.methodName = methodName;
    return this;
  }

  public String getNote() {
    return note;
  }

  public JournalEntry note(String note) {
    this.note = note;
    return this;
  }

  public Map getMutations() {
    return mutations;
  }

  public JournalEntry mutations(Map mutations) {
    this.mutations = mutations;
    return this;
  }

  public Instant getWhen() {
    return when;
  }

  public JournalEntry when(Instant when) {
    this.when = when;
    return this;
  }

  public JournalEntryModel toModel() {
    return new JournalEntryModel()
        .id(getId())
        .resourceKey(getResourceKey())
        .className(getClassName())
        .entryType(
            JournalEntryModel.EntryTypeEnum.fromValue(getEntryType().toString().toUpperCase()))
        .resourceType(IamResourceTypeEnum.fromValue(getResourceType().toString().toUpperCase()))
        .methodName(getMethodName())
        .user(getUser())
        .mutation(getMutations())
        .note(getNote())
        .when(getWhen().toString());
  }
}
