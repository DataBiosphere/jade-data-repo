package bio.terra.service.auth.iam;

import java.time.Instant;
import java.util.Set;

public record AuthorizedCacheValue(Instant timeout, Set<IamAction> actions) {}
