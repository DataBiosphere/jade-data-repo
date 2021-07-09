package bio.terra.service.filedata.google.firestore;

import java.util.ArrayList;
import java.util.List;

// POJO for mapping to and from /jade-testdata/encodetest/file.json
public class EncodeFileOut {
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
  private String file_ref;
  private String file_index_ref;
  private List<String> file_array;
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

  public EncodeFileOut() {}

  public EncodeFileOut(EncodeFileIn encodeFileIn, String bamRef, String bamiRef) {
    file_id = encodeFileIn.getFile_id();
    assay_type = encodeFileIn.getAssay_type();
    biosamples = encodeFileIn.getBiosamples();
    biosample_term_id = encodeFileIn.getBiosample_term_id();
    biosample_type = encodeFileIn.getBiosample_type();
    cell_type = encodeFileIn.getCell_type();
    data_quality_category = encodeFileIn.getData_quality_category();
    data_source = encodeFileIn.getData_source();
    data_type = encodeFileIn.getData_type();
    date_file_created = encodeFileIn.getDate_file_created();
    derived_from_exp = encodeFileIn.getDerived_from_exp();
    derived_from_ref = encodeFileIn.getDerived_from_ref();
    dna_library_ids = encodeFileIn.getDna_library_ids();
    donor_id = encodeFileIn.getDonor_id();
    experiments = encodeFileIn.getExperiments();
    file_available_in_gcs = encodeFileIn.isFile_available_in_gcs();
    file_format = encodeFileIn.getFile_format();
    file_format_subtype = encodeFileIn.getFile_format_subtype();
    file_ref = bamRef;
    file_index_ref = bamiRef;
    file_array = new ArrayList<>();
    if (bamRef != null) {
      file_array.add(bamRef);
    }
    if (bamiRef != null) {
      file_array.add(bamiRef);
    }
    file_size_mb = encodeFileIn.getFile_size_mb();
    labs_generating_data = encodeFileIn.getLabs_generating_data();
    md5sum = encodeFileIn.getMd5sum();
    more_info = encodeFileIn.getMore_info();
    paired_end_sequencing = encodeFileIn.isPaired_end_sequencing();
    percent_aligned_reads = encodeFileIn.getPercent_aligned_reads();
    percent_duplicated_reads = encodeFileIn.getPercent_duplicated_reads();
    read_count = encodeFileIn.getRead_count();
    read_length = encodeFileIn.getRead_length();
    reference_genome_assembly = encodeFileIn.getReference_genome_assembly();
    replicate_ids = encodeFileIn.getReplicate_ids();
    target_of_assay = encodeFileIn.getTarget_of_assay();
  }

  public String getFile_id() {
    return file_id;
  }

  public EncodeFileOut file_id(String file_id) {
    this.file_id = file_id;
    return this;
  }

  public String getAssay_type() {
    return assay_type;
  }

  public EncodeFileOut assay_type(String assay_type) {
    this.assay_type = assay_type;
    return this;
  }

  public String getBiosamples() {
    return biosamples;
  }

  public EncodeFileOut biosamples(String biosamples) {
    this.biosamples = biosamples;
    return this;
  }

  public String getBiosample_term_id() {
    return biosample_term_id;
  }

  public EncodeFileOut biosample_term_id(String biosample_term_id) {
    this.biosample_term_id = biosample_term_id;
    return this;
  }

  public String getBiosample_type() {
    return biosample_type;
  }

  public EncodeFileOut biosample_type(String biosample_type) {
    this.biosample_type = biosample_type;
    return this;
  }

  public String getCell_type() {
    return cell_type;
  }

  public EncodeFileOut cell_type(String cell_type) {
    this.cell_type = cell_type;
    return this;
  }

  public String getData_quality_category() {
    return data_quality_category;
  }

  public EncodeFileOut data_quality_category(String data_quality_category) {
    this.data_quality_category = data_quality_category;
    return this;
  }

  public String getData_source() {
    return data_source;
  }

  public EncodeFileOut data_source(String data_source) {
    this.data_source = data_source;
    return this;
  }

  public String getData_type() {
    return data_type;
  }

  public EncodeFileOut data_type(String data_type) {
    this.data_type = data_type;
    return this;
  }

  public String getDate_file_created() {
    return date_file_created;
  }

  public EncodeFileOut date_file_created(String date_file_created) {
    this.date_file_created = date_file_created;
    return this;
  }

  public String getDerived_from_exp() {
    return derived_from_exp;
  }

  public EncodeFileOut derived_from_exp(String derived_from_exp) {
    this.derived_from_exp = derived_from_exp;
    return this;
  }

  public String getDerived_from_ref() {
    return derived_from_ref;
  }

  public EncodeFileOut derived_from_ref(String derived_from_ref) {
    this.derived_from_ref = derived_from_ref;
    return this;
  }

  public String getDna_library_ids() {
    return dna_library_ids;
  }

  public EncodeFileOut dna_library_ids(String dna_library_ids) {
    this.dna_library_ids = dna_library_ids;
    return this;
  }

  public String getDonor_id() {
    return donor_id;
  }

  public EncodeFileOut donor_id(String donor_id) {
    this.donor_id = donor_id;
    return this;
  }

  public String getExperiments() {
    return experiments;
  }

  public EncodeFileOut experiments(String experiments) {
    this.experiments = experiments;
    return this;
  }

  public boolean isFile_available_in_gcs() {
    return file_available_in_gcs;
  }

  public EncodeFileOut file_available_in_gcs(boolean file_available_in_gcs) {
    this.file_available_in_gcs = file_available_in_gcs;
    return this;
  }

  public String getFile_format() {
    return file_format;
  }

  public EncodeFileOut file_format(String file_format) {
    this.file_format = file_format;
    return this;
  }

  public String getFile_format_subtype() {
    return file_format_subtype;
  }

  public EncodeFileOut file_format_subtype(String file_format_subtype) {
    this.file_format_subtype = file_format_subtype;
    return this;
  }

  public String getFile_ref() {
    return file_ref;
  }

  public EncodeFileOut file_ref(String file_ref) {
    this.file_ref = file_ref;
    return this;
  }

  public String getFile_index_ref() {
    return file_index_ref;
  }

  public EncodeFileOut file_index_ref(String file_index_ref) {
    this.file_index_ref = file_index_ref;
    return this;
  }

  public List<String> getFile_array() {
    return file_array;
  }

  public EncodeFileOut file_array(List<String> file_array) {
    this.file_array = file_array;
    return this;
  }

  public long getFile_size_mb() {
    return file_size_mb;
  }

  public EncodeFileOut file_size_mb(long file_size_mb) {
    this.file_size_mb = file_size_mb;
    return this;
  }

  public String getLabs_generating_data() {
    return labs_generating_data;
  }

  public EncodeFileOut labs_generating_data(String labs_generating_data) {
    this.labs_generating_data = labs_generating_data;
    return this;
  }

  public String getMd5sum() {
    return md5sum;
  }

  public EncodeFileOut md5sum(String md5sum) {
    this.md5sum = md5sum;
    return this;
  }

  public String getMore_info() {
    return more_info;
  }

  public EncodeFileOut more_info(String more_info) {
    this.more_info = more_info;
    return this;
  }

  public boolean isPaired_end_sequencing() {
    return paired_end_sequencing;
  }

  public EncodeFileOut paired_end_sequencing(boolean paired_end_sequencing) {
    this.paired_end_sequencing = paired_end_sequencing;
    return this;
  }

  public double getPercent_aligned_reads() {
    return percent_aligned_reads;
  }

  public EncodeFileOut percent_aligned_reads(double percent_aligned_reads) {
    this.percent_aligned_reads = percent_aligned_reads;
    return this;
  }

  public double getPercent_duplicated_reads() {
    return percent_duplicated_reads;
  }

  public EncodeFileOut percent_duplicated_reads(double percent_duplicated_reads) {
    this.percent_duplicated_reads = percent_duplicated_reads;
    return this;
  }

  public long getRead_count() {
    return read_count;
  }

  public EncodeFileOut read_count(long read_count) {
    this.read_count = read_count;
    return this;
  }

  public long getRead_length() {
    return read_length;
  }

  public EncodeFileOut read_length(long read_length) {
    this.read_length = read_length;
    return this;
  }

  public String getReference_genome_assembly() {
    return reference_genome_assembly;
  }

  public EncodeFileOut reference_genome_assembly(String reference_genome_assembly) {
    this.reference_genome_assembly = reference_genome_assembly;
    return this;
  }

  public String getReplicate_ids() {
    return replicate_ids;
  }

  public EncodeFileOut replicate_ids(String replicate_ids) {
    this.replicate_ids = replicate_ids;
    return this;
  }

  public String getTarget_of_assay() {
    return target_of_assay;
  }

  public EncodeFileOut target_of_assay(String target_of_assay) {
    this.target_of_assay = target_of_assay;
    return this;
  }
}
