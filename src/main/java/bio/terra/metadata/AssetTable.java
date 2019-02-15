package bio.terra.metadata;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class AssetTable {
    private StudyTable studyTable;
    private List<AssetColumn> columns = new ArrayList<>();

    public StudyTable getStudyTable() {
        return studyTable;
    }
    public AssetTable setStudyTable(StudyTable studyTable) {
        this.studyTable = studyTable;
        return this;
    }

    public Collection<AssetColumn> getColumns() { return columns; }
    public AssetTable setColumns(List<AssetColumn> columns) { this.columns = columns; return this; }

    public Optional<StudyTableColumn> getStudyColumnByName(String name) {
        for (AssetColumn assetColumn : getColumns()) {
            StudyTableColumn studyColumn = assetColumn.getStudyColumn();
            if (StringUtils.equals(name, studyColumn.getName())) {
                return Optional.of(studyColumn);
            }
        }
        return Optional.empty();
    }

}
