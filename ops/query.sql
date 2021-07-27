WITH links AS
(SELECT project.datarepo_row_id AS project_datarepo_row_id, links.datarepo_row_id AS links_datarepo_row_id, links_id, links.project_id, project.content AS project_content, links.content AS links_content
FROM
`broad-jade-dev-data.hca_dev_20210614_secret_search_api_snapshot.project` AS project,
`broad-jade-dev-data.hca_dev_20210614_secret_search_api_snapshot.links` AS links
WHERE JSON_VALUE(project.content, '$.project_core.project_short_name') = 'PulmonaryFibrosisGSE135893'
AND project.project_id = links.project_id)
SELECT
GENERATE_UUID() uuid,
links.project_datarepo_row_id AS project_datarepo_row_id,
links.project_id AS project_id,
JSON_EXTRACT_SCALAR(project_content, '$.project_core.project_short_name') AS tim__rdfsc__label,
JSON_EXTRACT_SCALAR(project_content, '$.project_core.project_title') AS tim__dctc__title,
JSON_EXTRACT_SCALAR(project_content, '$.project_core.project_description') AS tim__dctc__description,
links.links_datarepo_row_id AS links_datarepo_row_id,
links.links_id AS links_id,
JSON_EXTRACT_SCALAR(link, '$.link_type') AS link_type,
JSON_EXTRACT_SCALAR(link, '$.process_type') AS process_type,
JSON_EXTRACT_SCALAR(link, '$.process_id') AS process_id,
JSON_EXTRACT_SCALAR(input, '$.input_type') AS input_type,
JSON_EXTRACT_SCALAR(input, '$.input_id') AS input_id,
JSON_EXTRACT_SCALAR(output, '$.output_type') AS output_type,
sequence_file.file_id AS output_id,
JSON_EXTRACT_SCALAR(link, '$.entity.entity_type') AS entity_type,
JSON_EXTRACT_SCALAR(link, '$.entity.entity_id') AS entity_id,
JSON_EXTRACT_SCALAR(files, '$.file_type') AS file_type,
JSON_EXTRACT_SCALAR(files, '$.file_id') AS file_id,
cell_suspension_id AS cell_suspension_id,
JSON_EXTRACT_SCALAR(cell_suspension.content, '$.biomaterial_core.biomaterial_id') AS tim__dctc__identifier,
JSON_EXTRACT_SCALAR(cell_suspension.content, '$.selected_cell_type') AS tim__a__terraa__corec__hasa__selecteda__cella__type,
JSON_EXTRACT_SCALAR(donor_organism.content, '$.biomaterial_core.biomaterial_id') AS tim__provc__wasa__deriveda__from,
JSON_EXTRACT_SCALAR(donor_organism.content, '$.sex') AS tim__a__terraa__corec__hasa__sex,
JSON_EXTRACT_SCALAR(donor_organism.content, '$.organism_age') AS organism_age,
JSON_EXTRACT_SCALAR(donor_organism.content, '$.organism_age_unit.text') AS tim__a__terraa__corec__hasa__agea__unit,
JSON_EXTRACT_SCALAR(donor_organism_species, '$.text') AS tim__a__terraa__corec__hasa__organisma__type,
JSON_EXTRACT_SCALAR(specimen_from_organism.content, '$.organ.text') AS tim__a__terraa__corec__hasa__anatomicala__site,
JSON_EXTRACT_SCALAR(diseases, '$.text') AS tim__a__terraa__corec__hasa__disease,
JSON_EXTRACT_SCALAR(library_preparation_protocol.content, '$.library_construction_method.text') AS tim__a__terraa__corec__hasa__librarya__prep,
JSON_EXTRACT_SCALAR(library_preparation_protocol.content, '$.library_construction_method.ontology') AS library_construction_method_ontology,
JSON_EXTRACT_SCALAR(library_preparation_protocol.content, '$.library_construction_method.ontology_label') AS library_construction_method_ontology_label
FROM links
LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(links.links_content, '$.links')) AS link
LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(link, '$.inputs')) AS input
LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(link, '$.outputs')) AS output
LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(link, '$.files')) AS files
LEFT JOIN `broad-jade-dev-data.hca_dev_20210614_secret_search_api_snapshot.cell_suspension` AS cell_suspension ON JSON_EXTRACT_SCALAR(input, '$.input_id') = cell_suspension_id
LEFT JOIN `broad-jade-dev-data.hca_dev_20210614_secret_search_api_snapshot.donor_organism` AS donor_organism ON JSON_EXTRACT_SCALAR(input, '$.input_id') = donor_organism_id
LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(donor_organism.content, '$.genus_species')) AS donor_organism_species
LEFT JOIN `broad-jade-dev-data.hca_dev_20210614_secret_search_api_snapshot.specimen_from_organism` AS specimen_from_organism ON JSON_EXTRACT_SCALAR(input, '$.input_id') = specimen_from_organism_id
LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(specimen_from_organism.content, '$.diseases')) AS diseases
LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(link, '$.protocols')) AS protocols
LEFT JOIN `broad-jade-dev-data.hca_dev_20210614_secret_search_api_snapshot.library_preparation_protocol` AS library_preparation_protocol ON JSON_EXTRACT_SCALAR(protocols, '$.protocol_id') = library_preparation_protocol_id
LEFT JOIN `broad-jade-dev-data.hca_dev_20210614_secret_search_api_snapshot.sequence_file` AS sequence_file ON JSON_EXTRACT_SCALAR(output, '$.output_id') = sequence_file_id
;
