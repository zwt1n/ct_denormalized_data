/*
* 求職者基本情報
* データの定義
** CT登録の求職者の全データ
** 一部行動属性含む（登録初期ログイン率, 初日興味ある, CRS/ESS_PS受信数, 応募数、HS数）
** 注意！PSについては、直近1年のデータに限る
** 1求職者ID 1レコード
*/
with login as (
  -- 登録2～7日目ログイン
  select
    c.candidate_id
    ,count(distinct to_char(clh.login_tm,'yyyy-mm-dd'))
    ,cast(count(distinct to_char(clh.login_tm,'yyyy-mm-dd')) as dec)/6 as login_2_7_ratio
  from
    candidate c 
    left join candidate_login_history clh on c.candidate_id = clh.cid
      --登録2日目～7日目ログイン
      and c.ins_datetime::date < clh.login_tm::date
      and clh.login_tm::date <= date_add('day',6,c.ins_datetime::date)
  group by
    c.candidate_id
),

flw as(
  -- 当日興味あり
  select
    c.candidate_id
    ,count(rcd.conversion_tm) as flw_1_cnt
  from
    candidate c 
    left join recruiter_correspond_data rcd on c.candidate_id = rcd.candidate_id
      --登録当日興味ある
      and c.ins_datetime::date = rcd.conversion_tm::date
      --and rcd.conversion_tm::date <= date_add('day',6,c.ins_datetime::date)
      and rcd.conversion_type_cd = 'FLW'    
  group by
    c.candidate_id
),

apl as(
  -- 応募数
  select
    c.candidate_id
    ,count(cp.ins_datetime) as apl_cnt
  from
    candidate c 
    left join candidate_progress cp on c.candidate_id = cp.candidate_id
  group by
    c.candidate_id
),

hs as (
  -- HS数
  select
    c.candidate_id
    ,count(cpl.ins_datetime) as hs_cnt
  from
    candidate c 
    left join candidate_progress cp on c.candidate_id = cp.candidate_id
      left join candidate_progress_log cpl on cp.candidate_progress_id = cpl.candidate_progress_id
        and cpl.CANDIDATE_PROGRESS_STATUS_CD in('ITV__','MTADJ')
        and cpl.del_flg = 'N'
  group by
    c.candidate_id
),

ps as (
  -- PS受信フラグ
  -- PS受信数
  select
     c.candidate_id
    ,count(distinct case when ps.company_type_cd = 'CRS' then ps.message_thread_id end) as crs_ps_cnt
    ,count(distinct case when ps.company_type_cd = 'ESS' then ps.message_thread_id end) as ess_ps_cnt
  from
    candidate c 
    left join (
        select distinct
          mt.message_thread_id
          ,mt.scout_sent_tm
          ,mt.candidate_id
          ,mt.company_type_cd
        from
          message_thread mt
          inner join message m on mt.message_thread_id = m.message_thread_id
            and mt.scout_sent_tm = m.sent_tm
            and m.from_recruiter = 'TRUE'
            and m.sent_tm >= date_add('month', -13, date_trunc('month',convert_timezone('JST',current_date)))
        where
          mt.scout_sent_tm >= date_add('month', -13, date_trunc('month',convert_timezone('JST',current_date)))
          and mt.create_trigger_type_cd = 'PSC'
    )ps on c.candidate_id = ps.candidate_id
  group by
    c.candidate_id
),

c as (
  select
    c.candidate_id
    ,c.ins_datetime
    ,c.gender_cd
    ,DATEDIFF(YEAR, c.birth_date, GETDATE()) + (CASE WHEN DATE_PART (doy, c.birth_date) > DATE_PART (doy, GETDATE()) THEN - 1 ELSE 0 END) as age
    ,msi.school_rank_cd
    ,cr.candidate_rank_cd
    ,ma.area_name
    ,case when (split_part(cpi.tracking_code,'\_',1) is null or split_part(cpi.tracking_code,'\_',1) in ('Organic','app')) 
          then 'Organic' else split_part(cpi.tracking_code,'\_',1) end as trcd
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
          else null end as JOB_GROUP_TYPE_NAME  
    ,mjt.job_type_name
    ,case when mbt.business_type_group_cd = 'IA' then 'IT・インターネット・ゲーム・通信'
          when mbt.business_type_group_cd = 'IB' then 'メーカー'
          when mbt.business_type_group_cd = 'IC' then 'コンサルティング'
          when mbt.business_type_group_cd = 'ID' then '商社・流通・小売・サービス'
          when mbt.business_type_group_cd = 'IE' then '広告・マスコミ・エンターテインメント'
          when mbt.business_type_group_cd = 'IF' then '金融・不動産・建設'
          when mbt.business_type_group_cd = 'IG' then '医薬・医療・バイオ・メディカル'
          when mbt.business_type_group_cd = 'IH' then 'インフラ・公共・その他'
          else mbt.business_type_name end as business_group_type_name
    ,mbt.business_type_name
    ,ci.code_alias as income
    ,ceb.code_alias as education_bg
    ,r.job_change_duration_cd
    ,r.toeic_score
    ,r.toefl_score
    ,r.company_count
    ,LEN(NVL(rc.company_business, '') || NVL(rc.business_content, '') || NVL(rc.appeal_point, '') || NVL(rc.free_text, '')) as rsume_char_cnt
    ,c.candidate_status_cd
  from
    candidate c
      left join candidate_personal_info cpi on c.candidate_id = cpi.candidate_id
      left join candidate_rating cr on c.candidate_id = cr.candidate_id
      left join mst_area ma on c.area_cd = ma.mst_area_cd
  
      left join resume r on c.candidate_id = r.candidate_id
        left join cls_education_bg ceb on r.education_bg_cd = ceb.education_bg_cd
        left join cls_income ci on r.income_cd = ci.income_cd
        left join (
          -- resume_education_backgroundのresume_id重複削除
          select
            resume_id
            ,education_bg_cd
            ,education_name
            ,graduation_year
            ,course_type_cd
            ,school_master_id
            ,drop_flg
            ,expected_flg
          from
            resume_education_background
          where
            display_no = '1'
          group by
            resume_id
            ,education_bg_cd
            ,education_name
            ,graduation_year
            ,course_type_cd
            ,school_master_id
            ,drop_flg
            ,expected_flg
          ) reb
          on r.resume_id = reb.resume_id
            left join mst_school_info msi on reb.school_master_id = msi.mst_school_info_id
  
        left join resume_company rc on r.resume_id = rc.resume_id and rc.display_no = '1'
        left join resume_business rb on r.resume_id = rb.resume_id and rb.display_no = '1'
          left join mst_business_type mbt on rb.resume_business_type_cd  = mbt.business_type_cd
        left join resume_job rj on r.resume_id = rj.resume_id and rj.display_no = '1'
          left join mst_job_type mjt on rj.resume_job_type_cd  = mjt.job_type_cd
)

select
  -- static
  c.candidate_id as c_candidate_id
  ,c.ins_datetime as c_reg_datetime
  ,c.gender_cd as c_gender_cd
  ,c.age as c_age
  ,c.school_rank_cd as c_school_rank_cd
  ,c.candidate_rank_cd as c_candidate_rank_cd
  ,c.JOB_GROUP_TYPE_NAME as c_recent_g_job
  ,c.job_type_name as c_recent_job
  ,c.business_group_type_name as c_recent_g_industory
  ,c.business_type_name as c_recent_industory
  ,c.income as c_recent_income
  ,c.education_bg as c_education_bg
  ,c.job_change_duration_cd as c_job_change_duration_cd
  ,c.rsume_char_cnt as c_rsume_char_cnt
  ,c.candidate_status_cd as c_status_cd
  ,c.trcd as c_trcd
  ,c.area_name as c_area
  ,c.toeic_score as c_toeic
  ,c.toefl_score as c_toefl
  ,c.company_count as c_com_exp_cnt
  -- behavior  
  ,login.login_2_7_ratio as c_login_2_7_ratio
  ,flw.flw_1_cnt as c_flw_1_cnt
  ,apl.apl_cnt as c_apl_cnt
  ,hs.hs_cnt as c_hs_cnt
  ,ps.crs_ps_cnt as c_crs_ps_cnt
  ,ps.ess_ps_cnt as c_ess_ps_cnt
from
  c 
  left join login on c.candidate_id = login.candidate_id
  left join flw on c.candidate_id = flw.candidate_id
  left join apl on c.candidate_id = apl.candidate_id
  left join hs on c.candidate_id = hs.candidate_id
  left join ps on c.candidate_id = ps.candidate_id
;
