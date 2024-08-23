package bio.terra.service.load;

import java.util.UUID;

/**
 * Concurrent load operations (e.g. file ingests) to a dataset are not allowed to use the same load
 * tag.
 *
 * @param id the UUID representing this load lock
 * @param key the {@link LoadLockKey} to which this lock is scoped (load tag, dataset ID)
 * @param lockingFlightId the flight actively locking this load tag in this dataset
 */
public record LoadLock(UUID id, LoadLockKey key, String lockingFlightId) {}
