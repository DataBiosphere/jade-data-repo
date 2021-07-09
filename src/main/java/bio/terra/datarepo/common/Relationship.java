package bio.terra.common;

import java.util.UUID;

public class Relationship {

  private UUID id;
  private String name;
  private Column fromColumn;
  private Table fromTable;
  private Column toColumn;
  private Table toTable;

  public UUID getId() {
    return id;
  }

  public Relationship id(UUID id) {
    this.id = id;
    return this;
  }

  public String getName() {
    return name;
  }

  public Relationship name(String name) {
    this.name = name;
    return this;
  }

  public Table getFromTable() {
    return fromTable;
  }

  public Relationship fromTable(Table fromTable) {
    this.fromTable = fromTable;
    return this;
  }

  public Column getFromColumn() {
    return fromColumn;
  }

  public Relationship fromColumn(Column from) {
    this.fromColumn = from;
    return this;
  }

  public Table getToTable() {
    return toTable;
  }

  public Relationship toTable(Table toTable) {
    this.toTable = toTable;
    return this;
  }

  public Column getToColumn() {
    return toColumn;
  }

  public Relationship toColumn(Column to) {
    this.toColumn = to;
    return this;
  }
}
