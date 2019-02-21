package bio.terra.metadata;

import java.util.UUID;

public class Column {
    private UUID id;
    private Table table;
    private String name;
    private String type;

    public UUID getId() {
        return id;
    }

    public Column id(UUID id) {
        this.id = id;
        return this;
    }

    public Table getTable() {
        return table;
    }

    public Column table(Table table) {
        this.table = table;
        return this;
    }

    public String getName() {
        return name;
    }

    public Column name(String name) {
        this.name = name;
        return this;
    }

    public String getType() {
        return type;
    }

    public Column type(String type) {
        this.type = type;
        return this;
    }
}
