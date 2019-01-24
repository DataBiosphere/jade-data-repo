package bio.terra.pdao.bigquery;

import bio.terra.metadata.Study;
import bio.terra.metadata.StudyTable;
import bio.terra.metadata.StudyTableColumn;
import bio.terra.pdao.PrimaryDataAccess;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Profile("bigquery")
public class BigQueryPdao implements PrimaryDataAccess {

    private final BigQuery bigQuery;
    private final String projectId;
    private final String rowIdColumnName;
    private final String rowIdColumnDatatype;

    public BigQueryPdao(
            @Autowired BigQuery bigQuery,
            @Autowired String bigQueryProjectId,
            @Autowired String rowIdColumnName,
            @Autowired String rowIdColumnDatatype) {
        this.bigQuery = bigQuery;
        this.projectId = bigQueryProjectId;
        this.rowIdColumnName = rowIdColumnName;
        this.rowIdColumnDatatype = rowIdColumnDatatype;
    }

    @Override
    public boolean studyExists(String name) {
        DatasetId datasetId = DatasetId.of(projectId, name);
        Dataset dataset = bigQuery.getDataset(datasetId);
        return (dataset != null);
    }

    @Override
    public void createStudy(Study study) {
        createContainer(study.getName());
        for (StudyTable table : study.getTables()) {
            createTable(study.getName(), table);
        }
    }

    @Override
    public boolean deleteStudy(Study study) {
        return deleteContainer(study.getName());
    }

    private void createContainer(String name) {
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(name).build();
        bigQuery.create(datasetInfo);
    }

    private boolean deleteContainer(String name) {
        DatasetId datasetId = DatasetId.of(projectId, name);
        return bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
    }

    private void createTable(String containerName, StudyTable table) {
        TableId tableId = TableId.of(containerName, table.getName());
        Schema schema = buildSchema(table, true);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
    }

    private Schema buildSchema(StudyTable table, boolean addRowIdColumn) {
        List<Field> fieldList = new ArrayList<>();

        if (addRowIdColumn) {
            fieldList.add(Field.of(rowIdColumnName, translateType(rowIdColumnDatatype)));
        }

        for (StudyTableColumn column : table.getColumns()) {
            fieldList.add(Field.of(column.getName(), translateType(column.getType())));
        }

        return Schema.of(fieldList);
    }

    // TODO: Make an enum for the datatypes in swagger
    private LegacySQLTypeName translateType(String datatype) {
        String uptype = StringUtils.upperCase(datatype);
        switch (uptype) {
            case "BOOLEAN":   return LegacySQLTypeName.BOOLEAN;
            case "BYTES":     return LegacySQLTypeName.BYTES;
            case "DATE":      return LegacySQLTypeName.DATE;
            case "DATETIME":  return LegacySQLTypeName.DATETIME;
            case "FILEREF":   return LegacySQLTypeName.STRING;
            case "FLOAT":     return LegacySQLTypeName.FLOAT;
            case "INTEGER":   return LegacySQLTypeName.INTEGER;
            case "NUMERIC":   return LegacySQLTypeName.NUMERIC;
            //case "RECORD":    return LegacySQLTypeName.RECORD;
            case "STRING":    return LegacySQLTypeName.STRING;
            // One special to match the Postgres type
            case "TEXT":    return LegacySQLTypeName.STRING;
            case "TIME":      return LegacySQLTypeName.TIME;
            case "TIMESTAMP": return LegacySQLTypeName.TIMESTAMP;
            default: throw new IllegalArgumentException("Unknown datatype '" + datatype + "'");
        }
    }

}
