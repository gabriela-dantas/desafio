CREATE OR REPLACE VIEW bid_and_vacancies_view AS
SELECT gr.group_code,
    gr.group_deadline,
    gr.group_start_date,
    gr.group_closing_date,
    gr.per_max_embedded_bid,
    gr.next_adjustment_date,
    gr.current_assembly_date AS grp_current_assembly_date,
    gr.current_assembly_number AS grp_current_assembly_number,
    gv.vacancies,
    gv.info_date AS vacancies_info_date,
    bi.value,
    bi.assembly_date,
    bi.assembly_order,
    bi.info_date AS bid_info_date,
    bt.bid_type_code,
    bt.bid_type_desc,
    bvt.bid_value_type_code,
    bvt.bid_value_type_desc,
    adm.administrator_code,
    adm.administrator_desc
   FROM md_cota.pl_group gr
     JOIN md_cota.pl_administrator adm ON adm.administrator_id = gr.administrator_id
     LEFT JOIN md_cota.pl_group_vacancies gv ON gv.group_id = gr.group_id
     LEFT JOIN md_cota.pl_bid bi ON bi.group_id = gr.group_id
     LEFT JOIN md_cota.pl_bid_type bt ON bt.bid_type_id = bi.bid_type_id
     LEFT JOIN md_cota.pl_bid_value_type bvt ON bvt.bid_value_type_id = bi.bid_value_type_id
  WHERE gr.is_deleted IS FALSE AND (gv.is_deleted IS FALSE OR gv.is_deleted IS NULL) AND bi.is_deleted IS FALSE AND bt.is_deleted IS FALSE AND bvt.is_deleted IS FALSE AND gv.valid_to IS NULL;
