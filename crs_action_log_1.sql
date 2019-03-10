with cp as (
  select
      cp.ins_datetime
    , cp.candidate_progress_id
    , cp.apply_date
    , cp.candidate_id
    , cp.RECRUITER_COMPANY_ID
    , rc.company_type_cd
    , cp.job_id
    , case when cp.application_route_type_cd = 'SPE' then '公募・特集'
           when cp.application_route_type_cd in ('SCT','HSC') then 'DM'
           when cp.application_route_type_cd = 'APL' then '通常応募'
           when cp.application_route_type_cd = 'REC' then 'レコメンド応募'
           when cp.application_route_type_cd = 'PSC' and m.scout_s_class = 'FL' then '興味があるPS'
           when cp.application_route_type_cd = 'PSC' and m.scout_s_class not in ('FL') then 'その他PS'
           else 'Other' end as route_info
    , CANDIDATE_PROGRESS_DATA_JSON
  from
    CANDIDATE_PROGRESS cp
    inner join recruiter_company rc on cp.recruiter_company_id = rc.recruiter_company_id
    /*PSCのルート情報取得*/
    left join candidate_progress_message_info cpmi
      on cp.candidate_progress_id = cpmi.candidate_progress_id
      left join (
        select distinct
          mt.message_thread_id
          ,mt.create_trigger_type_cd
          ,mt.scout_s_class
        from
          message_thread mt
        where
          mt.scout_sent_tm >= '2017-08-01'
          and mt.create_trigger_type_cd = 'PSC'
          and mt.company_type_cd = 'CRS'
       )m on cpmi.message_thread_id = m.message_thread_id
  where
    /*ビズリーチ除外*/
    cp.recruiter_company_id <> 1 
    and cp.INS_DATETIME >= '2017-08-01'
)

/*求人公開*/
select
	  jscl.ins_datetime::date as act_date
	, '求人公開' as act_type
  , null::bigint as candidate_id
  , j.recruiter_company_id
  , jai.job_id
  , null as route_info
  , jscl.ins_datetime::date as date_for_join
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg
  , null::bigint as message_thread_id
  , null::bigint as replace_income
  , null::bigint as sf_kt_cnt
from
	job_status_change_log jscl
	left join job_admin_info jai using(job_admin_info_id)
	left join job j using(job_id)
	inner join recruiter_company rc on j.recruiter_company_id = rc.recruiter_company_id
	  and rc.company_type_cd = 'CRS'
where
	jscl.job_status_cd = 'PUB'
	and jscl.ins_datetime >= '2017-08-01'
  /*ビズリーチ除外*/
  and rc.recruiter_company_id <> 1

union all

/*リクルーターログイン日  1企業単位。一日複数回ログインしても1カウント*/
select distinct  
    rll.login_tm::date as act_date
	, 'ログイン' as act_type
  , null::bigint as candidate_id
  , rc.recruiter_company_id
  , null::bigint as job_id
  , null as route_info
  , rll.login_tm::date as date_for_join
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 
  , null::bigint as message_thread_id  
  , null::bigint as replace_income
  , null::bigint as sf_kt_cnt
from
  recruiter_login_log rll
  inner join recruiter r on r.recruiter_id = rll.recruiter_id
    inner join recruiter_company rc on rc.recruiter_company_id = r.recruiter_company_id
where
  rll.login_tm >= '2017-08-01'
  and rc.company_type_cd = 'CRS'
  /*ビズリーチ除外*/
  and rc.recruiter_company_id <> 1

union all

/*興味ある*/
select distinct
    rcd.conversion_tm::date as act_date
  , '興味ある' as act_type
  , rcd.candidate_id
  , rcd.recruiter_company_id
  , rcd.job_id
  , null as route_info
  , rcd.conversion_tm::date as date_for_join
  -- custom field
  , rcd.recruiter_done_flg as flw_done_flg
  , rcd.recruiter_scout_flg as flw_ps_snt_flg
  , null::bigint as message_thread_id
  , null::bigint as replace_income
  , null::bigint as sf_kt_cnt
from
  recruiter_correspond_data rcd
    inner join recruiter_company rc on rcd.recruiter_company_id = rc.recruiter_company_id
    and rc.company_type_cd = 'CRS'
where
  rcd.conversion_tm >= '2017-08-01'
  and rcd.conversion_type_cd = 'FLW'
  /*ビズリーチ除外*/
  and rcd.recruiter_company_id <> 1


union all

/*PS送信*/
select distinct
	  mt.scout_sent_tm::date as act_date
  , 'PS送信' as act_type
	, mt.candidate_id
	, mt.recruiter_company_id
	, mt.attached_job_id as job_id
	, case when mt.scout_s_class = 'FL' then '興味があるPS' else 'その他PS' end as route_info
  , mt.scout_sent_tm::date as date_for_join
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 
	, mt.message_thread_id
  , null::bigint as replace_income
  , null::bigint as sf_kt_cnt
from
	message_thread mt
  /*こっちの方がクエリが軽い。PS送信は企業側から必ず行うので、min()を使って企業側からの最初のメッセージを特定する必要はない*/
	inner join message m
	  on mt.message_thread_id = m.message_thread_id
	  and mt.scout_sent_tm = m.sent_tm
	  and m.from_recruiter = 'TRUE'
	  and m.sent_tm >= '2017-08-01'
where
  mt.create_trigger_type_cd = 'PSC'
  and mt.company_type_cd = 'CRS'
  and mt.scout_sent_tm >= '2017-08-01'
  and mt.recruiter_company_id <> 1 

union all

/*PS返信*/
select distinct
	  rep.reply_tm::date as act_date
  ,'PS返信' as act_type 	  
	, mt.candidate_id
	, mt.recruiter_company_id
	, mt.attached_job_id as job_id	
	, case when mt.scout_s_class = 'FL' then '興味があるPS' else 'その他PS' end as route_info
  , mt.scout_sent_tm::date as date_for_join
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 	
  , mt.message_thread_id
  , null::bigint as replace_income
  , null::bigint as sf_kt_cnt
from
  message_thread mt
  inner join (
  -- messageを使い、初回以降のやりとりの中で一番最初の求職者アクションを拾う(from_recruiter ='FALSE')
    select
      message_thread_id
      ,min(sent_tm) reply_tm
    from
      message m
    where
      from_recruiter ='FALSE'
      and m.sent_tm >= '2017-08-01'
    group by
      message_thread_id
  ) rep
    on mt.message_thread_id = rep.message_thread_id
where
  mt.create_trigger_type_cd = 'PSC'
  and mt.cn_scout_first_view_tm is not null
  and mt.company_type_cd = 'CRS'
  and mt.scout_sent_tm >= '2017-08-01'
  and mt.recruiter_company_id <> 1 

union all

/*応募*/
select
  cp.apply_date as act_date
  ,'応募' as act_type
  , cp.candidate_id
  , cp.recruiter_company_id
  , cp.job_id
  , cp.route_info
  , cp.apply_date as date_for_join
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 	
  , null::bigint as message_thread_id  
  , null::bigint as replace_income
  , null::bigint as sf_kt_cnt
from
  cp

union all

/*HS*/
select
	cpl.ins_datetime::date as act_date	
  ,'HS' as act_type
	, cp.candidate_id
	, cp.recruiter_company_id
	, cp.job_id	
	, cp.route_info
  , cp.apply_date as date_for_join
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 	
  , null::bigint as message_thread_id  
  , null::bigint as replace_income
  , null::bigint as sf_kt_cnt
from
  cp
  inner join candidate_progress_log cpl
    on cp.CANDIDATE_PROGRESS_ID = cpl.CANDIDATE_PROGRESS_ID
where
  cpl.DEL_FLG = 'N'
  and cpl.CANDIDATE_PROGRESS_STATUS_CD in ('ITV__')
  
union all

/*1面設定  設定日ベース*/
select
  to_date(case when json_extract_path_text(cp.CANDIDATE_PROGRESS_DATA_JSON,'firstInterviewSetTime') <> '' 
    then date_trunc('day',timestamp 'epoch' + cast(json_extract_path_text(cp.CANDIDATE_PROGRESS_DATA_JSON,'firstInterviewSetTime') as bigint) 
    * interval '0.001 second' + interval  '9 hour') else null end,'yyyy-mm-dd') as act_date
  , '1面設定' as act_type
  , cp.candidate_id
  , cp.RECRUITER_COMPANY_ID
  , cp.job_id
  , cp.route_info
  , cp.apply_date as date_for_join  
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 
  , null::bigint as message_thread_id  
  , null::bigint as replace_income
  , null::bigint as sf_kt_cnt  
from
  cp
where
  json_extract_path_text(cp.CANDIDATE_PROGRESS_DATA_JSON,'firstInterviewSetTime') <> ''

union all

/*内定*/
select
  cpl.INS_DATETIME::date as act_date
  ,'内定' as act_type
  , cp.candidate_id
  , cp.RECRUITER_COMPANY_ID
  , cp.job_id
  , cp.route_info
  , cp.apply_date as date_for_join  
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 
  , null::bigint as replace_income
  , null::bigint as message_thread_id  
  , null::bigint as sf_kt_cnt
from
  cp
  inner join candidate_progress_log cpl
    on cp.candidate_progress_id = cpl.candidate_progress_id
    and cpl.candidate_progress_status_cd = 'OFFER'
    and cpl.del_flg = 'N'

union all

/*ネット決定 申請日ベース*/
select
  cr.INS_DATETIME::date as act_date
  ,'決定' as act_type
  , cp.candidate_id
  , cp.recruiter_company_id
  , cp.job_id
  , cp.route_info
  , cp.apply_date as date_for_join  
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 
  , null::bigint as message_thread_id  
  , cr.income as replace_income
  , null::bigint as sf_kt_cnt
from
  CANDIDATE_REPLACE cr
  inner join cp on cr.CANDIDATE_PROGRESS_ID = cp.CANDIDATE_PROGRESS_ID
    inner join recruiter_company rc on cp.recruiter_company_id = rc.recruiter_company_id
    inner join candidate c on cp.candidate_id = c.candidate_id
where
	/*「入社前辞退」を除外*/
	not exists(
	  select * 
	  from candidate_progress_log cpl
	  where
	    cp.candidate_progress_id = cpl.candidate_progress_id
	    and cpl.candidate_progress_status_cd = 'HIRNG'  
	    and cpl.del_flg = 'N'
	)

union all

/*ネット決定 承認日ベース*/
select
    kt.hassei_date as act_date
  , 'SF決定' act_type
  , cr.candidate_id    
  , cp.RECRUITER_COMPANY_ID
  , cr.job_id    
  , cp.route_info
  , cp.apply_date as date_for_join  
  -- custom field
  , null as flw_done_flg
  , null as flw_ps_snt_flg 
  , null::bigint as message_thread_id  
  , kt.sales_price as replace_income
  , (case when kt.sales_price is null then 0
          when kt.sales_product_type_cd = 'REP' then 1
          when kt.sales_product_type_cd = 'OFF' and kt2.sales_product_type_cd = 'RDC' then 0
          when kt.sales_product_type_cd = 'OFF' then -1
          when kt.sales_product_type_cd = 'BOR' and kt.previous_id is not null then 1
          when kt.sales_product_type_cd = 'BOR' and kt.previous_id is null then 0
          when kt.sales_product_type_cd = 'RDC' and kt.sales_enable_flg = 'N' then 0
     else 0 end) as sf_kt_cnt   /*決定(承認日ベース)集計用*/
from 
  hassei_sales kt
  inner join CANDIDATE_REPLACE CR
    ON kt.SALES_PRODUCT_ID = CR.CANDIDATE_REPLACE_ID
    left join hassei_sales kt2
      on kt.previous_id = kt2.hassei_sales_id
  -- 経路情報付加
  inner JOIN cp
    ON CR.CANDIDATE_PROGRESS_ID = cp.CANDIDATE_PROGRESS_ID
where
  kt.sales_remarks !~ 'ご利用プラン*'
  and kt.hassei_date >= '2017-08-01'
;
