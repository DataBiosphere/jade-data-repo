package bio.terra.service.job;

public enum LockBehaviorFlags {
    LOCK_ONLY_IF_OBJECT_EXISTS,
    LOCK_ONLY_IF_OBJECT_DOES_NOT_EXIST,
    LOCK_REGARDLESS;
}
