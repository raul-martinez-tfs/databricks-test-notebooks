SELECT *
FROM "tfsdl_edp_common_dims"."tbl_edp_consumable_layer_record_cnt" 
where lower(project) like '%lighthouse%'
-- order by date desc
;