package bio.terra.service.filedata.google.firestore;

// POJO for mapping to and from /jade-testdata/encodetest/file.json
public class EncodeFileIn {
    private String file_id;
    private String assay_type;
    private String biosamples;
    private String biosample_term_id;
    private String biosample_type;
    private String cell_type;
    private String data_quality_category;
    private String data_source;
    private String data_type;
    private String date_file_created;
    private String derived_from_exp;
    private String derived_from_ref;
    private String dna_library_ids;
    private String donor_id;
    private String experiments;
    private boolean file_available_in_gcs;
    private String file_format;
    private String file_format_subtype;
    private String file_gs_path;
    private String file_index_gs_path;
    private long file_size_mb;
    private String labs_generating_data;
    private String md5sum;
    private String more_info;
    private boolean paired_end_sequencing;
    private double percent_aligned_reads;
    private double percent_duplicated_reads;
    private long read_count;
    private long read_length;
    private String reference_genome_assembly;
    private String replicate_ids;
    private String target_of_assay;

    public String getFile_id() {
        return file_id;
    }

    public EncodeFileIn file_id(String file_id) {
        this.file_id = file_id;
        return this;
    }

    public String getAssay_type() {
        return assay_type;
    }

    public EncodeFileIn assay_type(String assay_type) {
        this.assay_type = assay_type;
        return this;
    }

    public String getBiosamples() {
        return biosamples;
    }

    public EncodeFileIn biosamples(String biosamples) {
        this.biosamples = biosamples;
        return this;
    }

    public String getBiosample_term_id() {
        return biosample_term_id;
    }

    public EncodeFileIn biosample_term_id(String biosample_term_id) {
        this.biosample_term_id = biosample_term_id;
        return this;
    }

    public String getBiosample_type() {
        return biosample_type;
    }

    public EncodeFileIn biosample_type(String biosample_type) {
        this.biosample_type = biosample_type;
        return this;
    }

    public String getCell_type() {
        return cell_type;
    }

    public EncodeFileIn cell_type(String cell_type) {
        this.cell_type = cell_type;
        return this;
    }

    public String getData_quality_category() {
        return data_quality_category;
    }

    public EncodeFileIn data_quality_category(String data_quality_category) {
        this.data_quality_category = data_quality_category;
        return this;
    }

    public String getData_source() {
        return data_source;
    }

    public EncodeFileIn data_source(String data_source) {
        this.data_source = data_source;
        return this;
    }

    public String getData_type() {
        return data_type;
    }

    public EncodeFileIn data_type(String data_type) {
        this.data_type = data_type;
        return this;
    }

    public String getDate_file_created() {
        return date_file_created;
    }

    public EncodeFileIn date_file_created(String date_file_created) {
        this.date_file_created = date_file_created;
        return this;
    }

    public String getDerived_from_exp() {
        return derived_from_exp;
    }

    public EncodeFileIn derived_from_exp(String derived_from_exp) {
        this.derived_from_exp = derived_from_exp;
        return this;
    }

    public String getDerived_from_ref() {
        return derived_from_ref;
    }

    public EncodeFileIn derived_from_ref(String derived_from_ref) {
        this.derived_from_ref = derived_from_ref;
        return this;
    }

    public String getDna_library_ids() {
        return dna_library_ids;
    }

    public EncodeFileIn dna_library_ids(String dna_library_ids) {
        this.dna_library_ids = dna_library_ids;
        return this;
    }

    public String getDonor_id() {
        return donor_id;
    }

    public EncodeFileIn donor_id(String donor_id) {
        this.donor_id = donor_id;
        return this;
    }

    public String getExperiments() {
        return experiments;
    }

    public EncodeFileIn experiments(String experiments) {
        this.experiments = experiments;
        return this;
    }

    public boolean isFile_available_in_gcs() {
        return file_available_in_gcs;
    }

    public EncodeFileIn file_available_in_gcs(boolean file_available_in_gcs) {
        this.file_available_in_gcs = file_available_in_gcs;
        return this;
    }

    public String getFile_format() {
        return file_format;
    }

    public EncodeFileIn file_format(String file_format) {
        this.file_format = file_format;
        return this;
    }

    public String getFile_format_subtype() {
        return file_format_subtype;
    }

    public EncodeFileIn file_format_subtype(String file_format_subtype) {
        this.file_format_subtype = file_format_subtype;
        return this;
    }

    public String getFile_gs_path() {
        return file_gs_path;
    }

    public EncodeFileIn file_gs_path(String file_gs_path) {
        this.file_gs_path = file_gs_path;
        return this;
    }

    public String getFile_index_gs_path() {
        return file_index_gs_path;
    }

    public EncodeFileIn file_index_gs_path(String file_index_gs_path) {
        this.file_index_gs_path = file_index_gs_path;
        return this;
    }

    public long getFile_size_mb() {
        return file_size_mb;
    }

    public EncodeFileIn file_size_mb(long file_size_mb) {
        this.file_size_mb = file_size_mb;
        return this;
    }

    public String getLabs_generating_data() {
        return labs_generating_data;
    }

    public EncodeFileIn labs_generating_data(String labs_generating_data) {
        this.labs_generating_data = labs_generating_data;
        return this;
    }

    public String getMd5sum() {
        return md5sum;
    }

    public EncodeFileIn md5sum(String md5sum) {
        this.md5sum = md5sum;
        return this;
    }

    public String getMore_info() {
        return more_info;
    }

    public EncodeFileIn more_info(String more_info) {
        this.more_info = more_info;
        return this;
    }

    public boolean isPaired_end_sequencing() {
        return paired_end_sequencing;
    }

    public EncodeFileIn paired_end_sequencing(boolean paired_end_sequencing) {
        this.paired_end_sequencing = paired_end_sequencing;
        return this;
    }

    public double getPercent_aligned_reads() {
        return percent_aligned_reads;
    }

    public EncodeFileIn percent_aligned_reads(double percent_aligned_reads) {
        this.percent_aligned_reads = percent_aligned_reads;
        return this;
    }

    public double getPercent_duplicated_reads() {
        return percent_duplicated_reads;
    }

    public EncodeFileIn percent_duplicated_reads(double percent_duplicated_reads) {
        this.percent_duplicated_reads = percent_duplicated_reads;
        return this;
    }

    public long getRead_count() {
        return read_count;
    }

    public EncodeFileIn read_count(long read_count) {
        this.read_count = read_count;
        return this;
    }

    public long getRead_length() {
        return read_length;
    }

    public EncodeFileIn read_length(long read_length) {
        this.read_length = read_length;
        return this;
    }

    public String getReference_genome_assembly() {
        return reference_genome_assembly;
    }

    public EncodeFileIn reference_genome_assembly(String reference_genome_assembly) {
        this.reference_genome_assembly = reference_genome_assembly;
        return this;
    }

    public String getReplicate_ids() {
        return replicate_ids;
    }

    public EncodeFileIn replicate_ids(String replicate_ids) {
        this.replicate_ids = replicate_ids;
        return this;
    }

    public String getTarget_of_assay() {
        return target_of_assay;
    }

    public EncodeFileIn target_of_assay(String target_of_assay) {
        this.target_of_assay = target_of_assay;
        return this;
    }
}
