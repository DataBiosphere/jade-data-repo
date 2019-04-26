package bio.terra.pdao.bigquery;

import com.google.cloud.bigquery.Acl;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryContainerInfo {
    private String bqDatasetId;
    private Map<String, List<Acl>> studyToViewAcls = new HashMap();
    private String readersEmail;

    public BigQueryContainerInfo addViewAcls(String studyId, List<Acl> acls) {
        if (studyToViewAcls.containsKey(studyId)) {
            studyToViewAcls.get(studyId).addAll(acls);
        } else {
            studyToViewAcls.put(studyId, new ArrayList<>(acls));
        }
        return this;
    }

    public String getBqDatasetId() {
        return bqDatasetId;
    }

    public BigQueryContainerInfo bqDatasetId(String bqDatasetId) {
        this.bqDatasetId = bqDatasetId;
        return this;
    }

    public Map<String, List<Acl>> getStudyToViewAcls() {
        return studyToViewAcls;
    }

    public BigQueryContainerInfo studyToViewAcls(Map<String, List<Acl>> studyToViewAcls) {
        this.studyToViewAcls = studyToViewAcls;
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
            .append(studyToViewAcls, that.studyToViewAcls)
            .append(readersEmail, that.readersEmail)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .append(bqDatasetId)
            .append(studyToViewAcls)
            .append(readersEmail)
            .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("bqDatasetId", bqDatasetId)
            .append("studyToViewAcls", studyToViewAcls)
            .append("readersEmail", readersEmail)
            .toString();
    }
}
