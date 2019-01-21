package bio.terra.metadata;

import java.util.UUID;

public class Relationship {

    enum Cardinality {
        ONE, MANY, ZERO_OR_ONE, ONE_OR_MANY, ZERO_OR_MANY
    }

    private UUID id;
    private String name;
}
