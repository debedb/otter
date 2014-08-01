
INSERT INTO universal_source (db_name, title, description, source_type) values ('test1','test1','Test by GG', 'raw_data');

SELECT LAST_INSERT_ID() INTO @dataset_id;

INSERT INTO 
universal_property
(universal_source_id, title, name, type)
VALUES
(@dataset_id,'trn_id','trn_id','varchar'),
(@dataset_id, 'trn_date', 'trn_date', 'datetime'),
(@dataset_id, 'trn_type', 'trn_type', 'varchar'),
(@dataset_id, 'trn_crd_no', 'trn_crd_no', 'varchar'),
(@dataset_id, 'loc_code', 'loc_code', 'varchar'),
(@dataset_id, 'loc_description','loc_description','varchar'), 
(@dataset_id, 'trn_total_value', 'trn_total_value','varchar'), 
(@dataset_id, 'prd_code', 'prd_code', 'varchar'),
(@dataset_id, 'prd_description', 'prd_description','varchar'),  
(@dataset_id, 'tdt_qty', 'tdt_qty', 'real'),
(@dataset_id, 'tdt_value', 'tdt_value', 'real');



 
 