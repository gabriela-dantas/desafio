CREATE OR REPLACE VIEW md_cota.pl_quota_field_update_date_view AS
 SELECT qu.quota_id,
    qu.quota_code,
    qhf.quota_history_field_code,
    qfud.update_date,
    ds.data_source_code,
    ds.data_source_desc
   FROM md_cota.pl_quota qu
     JOIN md_cota.pl_quota_field_update_date qfud ON qfud.quota_id = qu.quota_id
     JOIN md_cota.pl_quota_history_field qhf ON qhf.quota_history_field_id = qfud.quota_history_field_id
     JOIN md_cota.pl_data_source ds ON ds.data_source_id = qfud.data_source_id
  WHERE qu.is_deleted IS FALSE AND qfud.is_deleted IS FALSE AND qhf.is_deleted IS FALSE AND ds.is_deleted IS FALSE
  ORDER BY qu.quota_id;
