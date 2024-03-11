CREATE OR REPLACE VIEW pl_asset_view AS
SELECT ass.asset_code,
    ass.asset_adm_code,
    ass.asset_desc,
    ass.asset_value,
    ass."PLAN",
    ass.administrator_fee,
    ass.fund_reservation_fee,
    sst.asset_type_code,
    sst.asset_type_code_ext,
    sst.asset_type_desc,
    gr.group_code,
    gr.group_deadline,
    gr.group_start_date,
    gr.group_closing_date,
    gr.per_max_embedded_bid,
    gr.next_adjustment_date,
    gr.current_assembly_date AS grp_current_assembly_date,
    gr.current_assembly_number AS grp_current_assembly_number,
    adm.administrator_code,
    adm.administrator_desc
   FROM md_cota.pl_asset ass
     JOIN md_cota.pl_group gr ON ass.group_id = gr.group_id
     JOIN md_cota.pl_asset_type sst ON sst.asset_type_id = ass.asset_type_id
     JOIN md_cota.pl_administrator adm ON adm.administrator_id = gr.administrator_id
  WHERE ass.is_deleted IS FALSE AND gr.is_deleted IS FALSE AND sst.is_deleted IS FALSE AND adm.is_deleted IS FALSE AND ass.valid_to IS NULL;
