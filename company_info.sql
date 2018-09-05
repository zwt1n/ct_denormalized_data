/*
* 最新の企業情報
* データの定義
** ビズリーチ除く
** ESS/CRS含む
** オプションを除く、メイン契約があった企業
** 1企業ID 1レコード
*/
with con as (
  select
    pr.purchase_receipt_id as b_contract_id
    ,pr.recruiter_company_id as b_company_id
    ,rc.company_name as b_company_name
    ,rc.company_type_cd as b_company_type
    ,ma.area_name as b_company_area    
    ,case when mbt.business_type_group_cd = 'IA' then 'IT・インターネット・ゲーム・通信'
          when mbt.business_type_group_cd = 'IB' then 'メーカー'
          when mbt.business_type_group_cd = 'IC' then 'コンサルティング'
          when mbt.business_type_group_cd = 'ID' then '商社・流通・小売・サービス'
          when mbt.business_type_group_cd = 'IE' then '広告・マスコミ・エンターテインメント'
          when mbt.business_type_group_cd = 'IF' then '金融・不動産・建設'
          when mbt.business_type_group_cd = 'IG' then '医薬・医療・バイオ・メディカル'
          when mbt.business_type_group_cd = 'IH' then 'インフラ・公共・その他'
          else mbt.business_type_name end as b_com_g_business
    ,mbt.business_type_name as b_com_business    
    ,pr.product_cd as b_contract_prd_cd
    ,pr.product_name as b_contract_prd
    ,pr.valid_from_date as b_contract_start_date
    ,pr.valid_to_date as b_contract_end_date
    -- 契約開始日が重複している怪しいレコード除去用(2015年以前の契約は、契約開始日が異なるが、契約終了日が同じ契約があるので注意)
    ,row_number()over(partition by pr.recruiter_company_id,pr.valid_from_date order by pr.ins_datetime) as row_num
  from
    purchase_receipt pr
    inner join recruiter_company rc on pr.recruiter_company_id = rc.recruiter_company_id
      left join mst_area ma on rc.area_cd = ma.mst_area_cd
      left join mst_business_type mbt on rc.business_type_cd = mbt.business_type_cd
  where
    product_category_cd = 'MAI'
    and del_flg = 'N'
    and pr.recruiter_company_id <> 1
),

tmp as (
  select
    b_company_id
    ,b_company_name
    ,b_company_type
    ,b_company_area
    ,b_com_g_business
    ,b_com_business
    ,b_contract_prd_cd
    ,b_contract_prd
    ,b_contract_start_date
    ,b_contract_end_date
    ,row_number()over(partition by b_company_id order by b_contract_start_date desc) as recent_contract
  from
    con
  where
    row_num = 1
),

select
   b_company_id
  ,b_company_name
  ,b_company_type
  ,b_company_area
  ,b_com_g_business
  ,b_com_business
  ,b_contract_prd_cd
  ,b_contract_prd
  ,b_contract_start_date as b_recent_contract_start_date
  ,b_contract_end_date as b_recent_contract_end_date
from
  tmp
where
  recent_contract = 1
;
