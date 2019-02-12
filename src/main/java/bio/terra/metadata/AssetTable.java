package bio.terra.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

}
