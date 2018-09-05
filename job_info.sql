/*
* 求人基本情報
* データの定義
** 1job_id 1レコード
** j_business_g_type/j_business_type : ESS求人の業種 or CRS企業の業種
** j_company_name : ESS求人の社名 or CRS企業の社名
** j_source_company : ESS求人はエージェント名、CRSは企業名
*/
with com as (
  select
     rc.recruiter_company_id
    ,rc.company_name
    ,rc.company_type_cd
    ,ma.area_name
    ,case when mbt.business_type_group_cd = 'IA' then 'IT・インターネット・ゲーム・通信'
          when mbt.business_type_group_cd = 'IB' then 'メーカー'
          when mbt.business_type_group_cd = 'IC' then 'コンサルティング'
          when mbt.business_type_group_cd = 'ID' then '商社・流通・小売・サービス'
          when mbt.business_type_group_cd = 'IE' then '広告・マスコミ・エンターテインメント'
          when mbt.business_type_group_cd = 'IF' then '金融・不動産・建設'
          when mbt.business_type_group_cd = 'IG' then '医薬・医療・バイオ・メディカル'
          when mbt.business_type_group_cd = 'IH' then 'インフラ・公共・その他'
          else null end as company_business_group_type_name
    ,mbt.business_type_name as company_business_type_name
  from
    recruiter_company rc
    left join mst_area ma on rc.area_cd = ma.mst_area_cd
    left join mst_business_type mbt
      on rc.business_type_cd = mbt.business_type_cd
),

select
  -- 求人付帯情報
  j.job_id as j_job_id
  ,j.job_name as j_job_name
  ,j.publish_start_tm as j_publish_start_tm
  ,j.job_publish_type_cd as j_publish_type_cd
  -- NULLはCRS企業の情報を付与する
  ,case when mbt.business_type_group_cd = 'IA' then 'IT・インターネット・ゲーム・通信'
        when mbt.business_type_group_cd = 'IB' then 'メーカー'
        when mbt.business_type_group_cd = 'IC' then 'コンサルティング'
        when mbt.business_type_group_cd = 'ID' then '商社・流通・小売・サービス'
        when mbt.business_type_group_cd = 'IE' then '広告・マスコミ・エンターテインメント'
        when mbt.business_type_group_cd = 'IF' then '金融・不動産・建設'
        when mbt.business_type_group_cd = 'IG' then '医薬・医療・バイオ・メディカル'
        when mbt.business_type_group_cd = 'IH' then 'インフラ・公共・その他'
        else com.company_business_group_type_name end as j_business_g_type
  -- NULLはCRS企業の情報を付与する
  ,case when mbt.business_type_name is null then com.company_business_group_type_name
        else null end as j_business_type
  ,case when mjt.JOB_GROUP_TYPE_CD = 'JA' then '営業'
        when mjt.JOB_GROUP_TYPE_CD = 'JB' then 'IT・WEB・エンジニア'
        when mjt.JOB_GROUP_TYPE_CD = 'JC' then 'マーケティング'
        when mjt.JOB_GROUP_TYPE_CD = 'JD' then 'メーカー'
        when mjt.JOB_GROUP_TYPE_CD = 'JE' then '経営・企画・管理'
        when mjt.JOB_GROUP_TYPE_CD = 'JF' then '販売・サービス'
        when mjt.JOB_GROUP_TYPE_CD = 'JG' then '資材・購買・物流'
        when mjt.JOB_GROUP_TYPE_CD = 'JH' then '建設・土木'
        when mjt.JOB_GROUP_TYPE_CD = 'JI' then 'コンサルタント'
        when mjt.JOB_GROUP_TYPE_CD = 'JJ' then '金融・不動産'
        when mjt.JOB_GROUP_TYPE_CD = 'JK' then 'メディカル'
        when mjt.JOB_GROUP_TYPE_CD = 'JL' then '専門職'
        when mjt.JOB_GROUP_TYPE_CD = 'JM' then 'クリエイティブ'
        when mjt.JOB_GROUP_TYPE_CD = 'JN' then 'その他'
       else null end as j_g_job_type
  ,mjt.job_type_name as j_job_type
  ,j.employment_type_cd as j_employment_type_cd
  ,j.income_lower as j_income_lower
  ,j.income_upper as j_income_upper
  ,j.income_lower/10000 || '万円' || '-' || j.income_upper/10000 || '万円'as j_income_range
  ,(cast(j.income_lower as dec(38,2)) + cast(j.income_upper as dec(38,2))) /2 as j_income_median
  ,case when jc.company_name is null then com.company_name else jc.company_name end  as j_company_name
  ,ma.area_name as j_main_area_name
  -- 以下求人掲載企業情報
  ,j.recruiter_company_id as j_recruiter_company_id
  ,com.company_type_cd as j_company_type_cd -- CRS or ESS求人の判別
  ,com.company_name as j_source_company -- CRSは企業名、ESSはエージェント名
from
  job j
  inner join com on j.recruiter_company_id = com.recruiter_company_id
  -- 職種
  left join mst_job_type mjt on j.job_type_cd = mjt.job_type_cd
  -- 業種
  left join job_business_type jbs -- ESS求人の業種
   on j.job_id = jbs.job_id
   and jbs.display_no = 1
   left join mst_business_type mbt on jbs.business_type_cd = mbt.business_type_cd
  
  left join mst_area ma on j.main_area_cd = ma.mst_area_cd
  left join job_company jc on j.job_id = jc.job_id -- ESS求人の業種
;
