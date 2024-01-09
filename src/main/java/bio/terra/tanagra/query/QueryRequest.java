package bio.terra.tanagra.query;

/** The request for a query to execute against a database backend. */
public record QueryRequest(Query query, ColumnHeaderSchema columnHeaderSchema) {}
