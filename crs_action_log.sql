/* Data set No.002 CRSアクションログ
* データの定義
** 直近13ヶ月のアクションに限定
** ビズリーチ除く
** 基本は、1アクション1レコード ※但し、以下企業は一部の期間で重複が発生する可能性あり
** 件数を数えるときは、act_idでdistinctを掛けるように 
** PSはそもそも添付求人数分件数が増えている。通数を数える時はact_idでdistinctを掛けるように

契約期間の重複があるモノ
  4321	株式会社ニトリ	2016/8/9	2037/12/31	2017/7/4	2017/11/14 重複期間 7485
  5941	ジブラルタ生命保険株式会社	2018/1/23	2018/7/22	2018/2/14	2018/8/13 重複期間 158
  1373	PwCコンサルティング合同会社	2017/1/17	2017/4/16	2017/3/2	2017/9/1 重複期間 45
  1373	PwCコンサルティング合同会社	2017/3/2	2017/9/1	2017/8/1	2018/7/31 重複期間 31
  4554	株式会社資生堂	2017/1/24	2017/7/23	2017/6/24	2017/12/23 重複期間 29
  833	  ソニー株式会社	2017/3/30	2017/4/27	2017/4/1	2018/3/31 重複期間 26
  5055	サクラシステムサービス株式会社	2017/1/10	2017/5/9	2017/4/30	2017/7/29 重複期間 9
  5150	株式会社クライマークス	2017/5/10	2017/8/9	2017/7/31	2018/7/30 重複期間 9
  4991	クックビズ株式会社	2016/12/8	2017/4/7	2017/4/3	2017/7/2  重複期間 4

契約終了日 = 次の契約開始日となっていて、一部重複するモノ
  580	  コードキャンプ株式会社 	2016/12/22	2017/4/21	2017/4/21	2017/7/20
  748	  株式会社グロービス	2016/8/7	2016/11/6	2016/11/6	2017/2/5
  748	  株式会社グロービス	2016/11/6	2017/2/5	2017/2/5	2017/5/4
  2563	株式会社インフォバーン	2016/11/12	2017/2/11	2017/2/11	2017/5/10
  4790	株式会社リードクリエイト	2016/9/26	2017/1/25	2017/1/25	2017/4/24
  4393	株式会社AMS	2016/10/22	2017/1/21	2017/1/21	2017/4/20
  3955	パーソルプロセス＆テクノロジー株式会社	2017/4/6	2017/10/5	2017/10/5	2018/4/4
  4722	株式会社CS-C	2016/8/24	2016/12/23	2016/12/23	2017/3/22
*/

------------------------------
-- 契約企業情報取得
------------------------------
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
    ,pr.upd_datetime
    -- 契約開始日、契約終了日が重複している怪しいレコード除去用
    ,row_number()over(partition by pr.recruiter_company_id,pr.valid_from_date order by pr.ins_datetime) as row_num
    ,row_number()over(partition by pr.recruiter_company_id,pr.valid_to_date order by pr.ins_datetime) as row_num2
  from
    purchase_receipt pr
    inner join recruiter_company rc on pr.recruiter_company_id = rc.recruiter_company_id
      left join mst_area ma on rc.area_cd = ma.mst_area_cd
      left join mst_business_type mbt on rc.business_type_cd = mbt.business_type_cd
  where
    product_category_cd = 'MAI'
    and del_flg = 'N'
    and rc.company_type_cd = 'CRS'
    and pr.recruiter_company_id <> 1
    and b_contract_start_date >= '2016-08-01'
),

con2 as (
  select
     b_contract_id
    ,b_company_id as recruiter_company_id
    ,b_company_name
    ,b_company_type
    ,b_company_area
    ,b_com_g_business
    ,b_com_business
    ,b_contract_prd_cd
    ,b_contract_prd
    ,b_contract_start_date
    ,b_contract_end_date
    ,row_number()over(partition by b_company_id order by b_contract_start_date desc) as b_recent_contract --1が直近
  from
    con
  where
    row_num = 1
    and row_num2 = 1
),

------------------------------
-- 進捗基本情報_経路含む
------------------------------
cp as (
  select
      cp.ins_datetime
    , cp.candidate_progress_id
    , cp.apply_date
    , cp.candidate_id
    , cp.RECRUITER_COMPANY_ID
    , cp.job_id
    , case when cp.application_route_type_cd = 'SPE' then '公募・特集'
           when cp.application_route_type_cd in ('SCT','HSC') then 'DM'
           when cp.application_route_type_cd = 'APL' then '通常応募'
           when cp.application_route_type_cd = 'REC' then 'レコメンド応募'
           when cp.application_route_type_cd = 'PSC' and m.scout_s_class = 'FL' then '1_興味があるPS'
           when cp.application_route_type_cd = 'PSC' and m.scout_s_class not in ('FL') then '2_その他PS'
           else 'Other' end as route_info
    , cp.candidate_progress_data_json
  from
    CANDIDATE_PROGRESS cp
    inner join recruiter_company rc on cp.recruiter_company_id = rc.recruiter_company_id
    -- PSCのルート情報取得
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
          mt.scout_sent_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
          and mt.create_trigger_type_cd = 'PSC'
          and mt.company_type_cd = 'CRS'
       )m on cpmi.message_thread_id = m.message_thread_id
  where
    rc.company_type_cd = 'CRS'
    -- ビズリーチ除外
    and cp.recruiter_company_id <> 1
),

------------------------------
-- アクションログ
------------------------------
tran as (
  ------------------------------------
  -- 契約ログ Daily 13ヶ月分
  ------------------------------------
  with con as (
    select
      pr.purchase_receipt_id as b_contract_id
      ,pr.recruiter_company_id as b_company_id
      ,pr.product_cd as b_contract_prd_cd
      ,pr.product_name as b_contract_prd
      ,pr.valid_from_date as b_contract_start_date
      ,pr.valid_to_date as b_contract_end_date
      -- 契約開始日が重複している怪しいレコード除去用(2015年以前の契約は、契約開始日が異なるが、契約終了日が同じ契約があるので注意)
      ,row_number()over(partition by pr.recruiter_company_id,pr.valid_from_date order by pr.ins_datetime) as row_num
    from
      purchase_receipt pr
      inner join recruiter_company rc on pr.recruiter_company_id = rc.recruiter_company_id
    where
      product_category_cd = 'MAI'
      and del_flg = 'N'
      and rc.company_type_cd = 'CRS'
      and pr.recruiter_company_id <> 1
  ),
  
  cal as(
    select distinct
      TARGET_DATE as cal_date
    from 
      ANALYTICAL_COMPANY_DAILY_DATA
    where 
      TARGET_DATE >= DATE_TRUNC('month', ADD_MONTHS(GETDATE(), -13))
      and TARGET_DATE <= GETDATE()
  )
  -- 対象日に契約があれば1レコード出力
  select
    cal.cal_date as act_date
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！   
    ,b_company_id as act_id
    ,'0_契約中' as act_type
    ,b_company_id as recruiter_company_id
    ,Null::bigint as job_id
    ,Null as route_info
    ,Null::bigint as replace_income
    ,b_contract_prd_cd
    ,b_contract_prd
  from
    cal
    left join con on (con.b_contract_start_date <= cal.cal_date and cal.cal_date < con.b_contract_end_date +1)
  where
    row_num = 1

union all

  ------------------------------------
  -- 求人公開
  ------------------------------------
  select
    jmin.publish_datetime as act_date
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！    
    ,j.job_id as act_id
    ,'1_求人公開' as act_type
    ,j.recruiter_company_id
    ,j.job_id
    ,Null as route_info
    ,Null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    job j
    inner join(
      select
        jai.job_id
        ,min(jscl.ins_datetime) as publish_datetime
      from
        job_admin_info jai
        inner join job_status_change_log jscl
            on jai.job_admin_info_id = jscl.job_admin_info_id
            and job_status_cd = 'PUB'
      group by jai.job_id
    ) jmin using(job_id)
    inner join recruiter_company rc
      on j.recruiter_company_id = rc.recruiter_company_id
  where
    rc.company_type_cd = 'CRS'   -- CRS企業のみ
    and jmin.publish_datetime >= DATE_TRUNC('MONTH',DATEADD(MONTH, -13,trunc(convert_timezone('JST', getdate()))))
    
union all

  ------------------------------------
  -- 初回自動スカウト設定
  ------------------------------------
  select
    jsi.ins_datetime as act_date
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！
    ,jsi.job_segment_id as act_id
    ,'2_自動SCT設定' as act_type
    ,j.recruiter_company_id
    ,j.job_id
    ,Null as route_info
    ,Null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    job_segment_info jsi
    inner join job j on j.job_id = jsi.job_id
      inner join recruiter_company rc on j.recruiter_company_id = rc.recruiter_company_id
  where
  -- スカウト種類がスカウトで配信中(ON)であること
    jsi.scout_type_cd='S'
    and jsi.job_segment_status_cd='SND'
    and rc.company_type_cd = 'CRS'
    and jsi.ins_datetime >= DATE_TRUNC('MONTH',DATEADD(MONTH, -13,trunc(convert_timezone('JST', getdate()))))

union all
  ------------------------------------
  -- リクルーターログイン日  1企業単位。一日複数回ログインしても1カウント
  ------------------------------------
  select distinct
    rll.login_tm::date as act_date
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！    
    ,max(rll.recruiter_login_log_id)over(partition by rc.recruiter_company_id,rll.login_tm::date) as act_id
    ,'3_ログイン' as act_type
    ,rc.recruiter_company_id
    ,Null::bigint as job_id
    ,Null as route_info
    ,Null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    recruiter_login_log rll
    inner join recruiter r on r.recruiter_id = rll.recruiter_id
      inner join recruiter_company rc on rc.recruiter_company_id = r.recruiter_company_id
  where
    rll.login_tm >= DATE_TRUNC('MONTH',DATEADD(MONTH, -13,trunc(convert_timezone('JST', getdate()))))
    and rc.company_type_cd = 'CRS'

union all
  ------------------------------------
  -- PS送信  添付求人数含む
  ------------------------------------
  
  select distinct
    mt.scout_sent_tm as act_date
    -- 通数カウント用 集計時にこのカラムでdistinct掛けること！
    ,mt.message_thread_id as act_id
    ,'4_PS送信' as act_type
    ,mt.recruiter_company_id
    -- 添付求人数集計時にはこのカラムでdistinct掛けること！
    ,mt.attached_job_id as job_id 
    ,mt.scout_s_class as route_info
    ,Null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from message_thread mt
       inner join message m
       on mt.message_thread_id = m.message_thread_id
       and mt.scout_sent_tm = m.sent_tm
       and m.from_recruiter = 'TRUE'
  where
    mt.create_trigger_type_cd = 'PSC'
    and company_type_cd = 'CRS'
    and mt.scout_sent_tm >= DATE_TRUNC('month', ADD_MONTHS(GETDATE(), -13))

union all
  ------------------------------------
  -- 足あと
  ------------------------------------
  select
    f.upd_datetime as action_tm
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！
    ,f.footprint_key as act_id
    ,'5_足あと' as action_flg
    ,f.company_id as recruiter_company_id
    ,f.job_id
    ,Null as route_info
    ,Null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    footprint f
    inner join job j on f.job_id = j.job_id
      inner join recruiter_company rc on j.recruiter_company_id = rc.recruiter_company_id
  where
    f.upd_datetime >= DATE_TRUNC('MONTH',DATEADD(MONTH, -13,trunc(convert_timezone('JST', getdate()))))
    and rc.company_type_cd = 'CRS'

union all
  ------------------------------------
  -- 興味がある
  ------------------------------------
  select distinct
    rcd.conversion_tm as action_tm
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！
    ,rcd.id as act_id
    ,'6_興味ある' as action_flg
    ,rcd.recruiter_company_id
    ,rcd.job_id
    ,Null as route_info
    ,Null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    recruiter_correspond_data rcd
    inner join recruiter_company rc on rcd.recruiter_company_id = rc.recruiter_company_id
     and rc.company_type_cd = 'CRS'
  where
    rcd.conversion_tm >= DATE_TRUNC('MONTH',DATEADD(MONTH, -13,trunc(convert_timezone('JST', getdate()))))
    and rcd.conversion_type_cd = 'FLW'

union all
  ------------------------------------
  -- 応募
  ------------------------------------
  select
    cp.ins_datetime as act_date
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！
    ,cp.candidate_progress_id as act_id
    ,'7_応募' as act_type
    ,cp.RECRUITER_COMPANY_ID
    ,cp.job_id
    ,cp.route_info
    ,null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    cp
  where
    cp.INS_DATETIME >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))


union all
  ------------------------------------
  -- HS
  ------------------------------------
  select
    cpl.INS_DATETIME as act_date
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！
    ,cp.candidate_progress_id as act_id
    ,'8_HS' act_type
    ,cp.RECRUITER_COMPANY_ID
    ,cp.job_id
    ,cp.route_info
    ,null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    cp
    inner join candidate_progress_log cpl
      on cp.CANDIDATE_PROGRESS_ID = cpl.CANDIDATE_PROGRESS_ID
  where
    cpl.DEL_FLG = 'N'
    and cpl.CANDIDATE_PROGRESS_STATUS_CD in ('ITV__')
    and cpl.INS_DATETIME >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))

union all
  ------------------------------------
  -- 1面設定  設定日ベース
  ------------------------------------
  select
    to_date(case when json_extract_path_text(cp.CANDIDATE_PROGRESS_DATA_JSON,'firstInterviewSetTime') <> '' 
      then date_trunc('day',timestamp 'epoch' + cast(json_extract_path_text(cp.CANDIDATE_PROGRESS_DATA_JSON,'firstInterviewSetTime') as bigint) 
      * interval '0.001 second' + interval  '9 hour') else null end,'yyyy-mm-dd') as act_date
     -- 件数カウント用 集計時にこのカラムでdistinct掛けること！
    ,cp.candidate_progress_id as act_id
    ,'9_1面設定' as act_type
    ,cp.RECRUITER_COMPANY_ID
    ,cp.job_id
    ,cp.route_info
    ,null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    cp
  where
    json_extract_path_text(cp.CANDIDATE_PROGRESS_DATA_JSON,'firstInterviewSetTime') <> ''
    and to_date(case when json_extract_path_text(cp.CANDIDATE_PROGRESS_DATA_JSON,'firstInterviewSetTime') <> '' 
        then date_trunc('day',timestamp 'epoch' + cast(json_extract_path_text(cp.CANDIDATE_PROGRESS_DATA_JSON,'firstInterviewSetTime') as bigint)
          * interval '0.001 second' + interval  '9 hour') else null end,'yyyy-mm-dd') >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))

union all
  ------------------------------------
  -- 内定
  ------------------------------------
  select
    cpl.INS_DATETIME as act_date
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！
    ,cp.candidate_progress_id as act_id
    ,'10_内定' as act_type
    ,cp.RECRUITER_COMPANY_ID
    ,cp.job_id
    ,cp.route_info
    ,null::bigint as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    cp
    inner join candidate_progress_log cpl
      on cp.candidate_progress_id = cpl.candidate_progress_id
      and cpl.candidate_progress_status_cd = 'OFFER'
      and cpl.del_flg = 'N'
 where
    cpl.INS_DATETIME >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))

union all
  ------------------------------------
  -- 決定 入社前辞退除く
  ------------------------------------
  select
    cr.INS_DATETIME as act_date
    -- 件数カウント用 集計時にこのカラムでdistinct掛けること！
    ,cr.candidate_replace_id as act_id
    ,'11_決定' as act_type
    ,cp.recruiter_company_id
    ,cp.job_id
    ,cp.route_info
    ,cr.income as replace_income
    ,Null as b_contract_prd_cd
    ,Null as b_contract_prd
  from
    CANDIDATE_REPLACE cr
    inner join cp on cr.CANDIDATE_PROGRESS_ID = cp.CANDIDATE_PROGRESS_ID
      inner join recruiter_company rc on cp.recruiter_company_id = rc.recruiter_company_id
      inner join candidate c on cp.candidate_id = c.candidate_id
   where
     cr.INS_DATETIME >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
    -- 「入社前辞退」を除外
    -- クエリエラーになるの場合あり
    and not exists(
      select * 
      from candidate_progress_log cpl
      where
        cp.candidate_progress_id = cpl.candidate_progress_id
        and cpl.candidate_progress_status_cd = 'HIRNG'  
        and cpl.del_flg = 'N'
    )
-- end tran
)

--merge
select
   tran.act_date
  ,tran.act_id
  ,tran.act_type
  ,tran.recruiter_company_id
  ,tran.job_id
  ,tran.route_info
  ,tran.replace_income
  ,tran.b_contract_prd_cd
  ,tran.b_contract_prd
  ,con2.b_contract_id
  ,con2.b_company_name
  ,con2.b_company_type
  ,con2.b_company_area
  ,con2.b_com_g_business
  ,con2.b_com_business
  ,con2.b_contract_prd_cd
  ,con2.b_contract_prd
  ,con2.b_contract_start_date
  ,con2.b_contract_end_date
  ,con2.b_recent_contract --1が直近     
from
  tran
  left join con2 
    on tran.recruiter_company_id = con2.recruiter_company_id
    and (con2.b_contract_start_date <= tran.act_date and tran.act_date <= con2.b_contract_end_date)
