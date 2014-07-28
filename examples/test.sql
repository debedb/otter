
insert into universal_source (db_name, title, description, source_type) values ('test1','test1','Test by GG', 'raw_data');

INSERT INTO 
universal_property
(universal_source_id, title, name, type)
VALUES
(11,'trn_id','trn_id','varchar'),
(11, 'trn_date', 'trn_date', 'datetime'),
(11, 'trn_type', 'trn_type', 'varchar'),
(11, 'trn_crd_no', 'trn_crd_no', 'varchar'),
(11, 'loc_code', 'loc_code', 'varchar'),
(11, 'loc_description','loc_description','varchar'), 
(11, 'trn_total_value', 'trn_total_value','varchar'), 
(11, 'prd_code', 'prd_code', 'varchar'),
(11, 'prd_description', 'prd_description','varchar'),  
(11, 'tdt_qty', 'tdt_qty', 'real'),
(11, 'tdt_value', 'tdt_value', 'real');

 
 