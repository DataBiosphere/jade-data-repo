package bio.terra.service.load;

import java.util.UUID;

/**
 * Concurrent load operations (e.g. file ingests) to a dataset are not allowed to use the same load
 * tag.
 *
 * @param loadTag tag identifying this load operation
 * @param datasetId dataset to which this load operation is being applied
 */
public record LoadLockKey(String loadTag, UUID datasetId) {}
