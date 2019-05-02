package bio.terra.pdao.bigquery;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO reviewers: i hate the name of this class. it's holding the info needed to authorize the views on the study
// i'm wondering if i should move this to the flight package for create dataset?
public class BigQueryContainerInfo {
    private String projectId;
    private String bqDatasetId;
    private Map<String, List<String>> studyToBQTableNames = new HashMap<>();
    private String readersEmail;

    public BigQueryContainerInfo addTables(String studyName, List<String> tables) {
        if (studyToBQTableNames.containsKey(studyName)) {
            studyToBQTableNames.get(studyName).addAll(tables);
        } else {
            studyToBQTableNames.put(studyName, new ArrayList<>(tables));
        }
        return this;
    }

    public String getProjectId() {
        return projectId;
    }

    public BigQueryContainerInfo projectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public String getBqDatasetId() {
        return bqDatasetId;
    }

    public BigQueryContainerInfo bqDatasetId(String bqDatasetId) {
        this.bqDatasetId = bqDatasetId;
        return this;
    }

    public Map<String, List<String>> getStudyToBQTableNames() {
        return studyToBQTableNames;
    }

    public BigQueryContainerInfo studyToBQTableNames(Map<String, List<String>> studyToBQTableNames) {
        this.studyToBQTableNames = studyToBQTableNames;
        return this;
    }

    public String getReadersEmail() {
        return readersEmail;
    }

    public BigQueryContainerInfo readersEmail(String readersEmail) {
        this.readersEmail = readersEmail;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        BigQueryContainerInfo that = (BigQueryContainerInfo) o;

        return new EqualsBuilder()
            .append(bqDatasetId, that.bqDatasetId)
            .append(studyToBQTableNames, that.studyToBQTableNames)
            .append(readersEmail, that.readersEmail)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(bqDatasetId)
            .append(studyToBQTableNames)
            .append(readersEmail)
            .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("bqDatasetId", bqDatasetId)
            .append("studyToBQTableNames", studyToBQTableNames)
            .append("readersEmail", readersEmail)
            .toString();
    }
}
