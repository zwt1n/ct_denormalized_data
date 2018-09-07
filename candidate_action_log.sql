/* Data set No.001 求職者アクションログ
データの定義
** ビズリーチへのアクションは除く
** ESS/CRS含む
** 直近6ヶ月のアクションデータ ※ESSのスカウトデータ量が多い為、半年に絞る
** candidate_id : 求職者ID
** act_date : YYYY-MM-DD
** act_type : 0_ログイン, 1_興味がある, 2_PS, 3_応募, 4_HS, 5_内定, 6_決定
** route_info : 自己応募(公募・特集、DM、通常応募、レコメンド応募), PS(興味があるPS, その他PS) ※応募以前はNULL
** job_id : 求人ID
** recruiter_company_id : 企業ID
** message_thread_id : スカウトのスレッドID act_typeがPSの時のみ値が入る。通数をカウントする際は、このIDユニークをかける
** ps_reply_tm : PS返信日 act_typeがPSの時のみ値が入る。
*/

------------------------------
-- 進捗基本情報_経路含む
------------------------------
with cp as (
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
  from
    CANDIDATE_PROGRESS cp
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
       )m on cpmi.message_thread_id = m.message_thread_id
  where
    -- ビズリーチ除外
    cp.recruiter_company_id <> 1  
),

tran as (
  ------------------------------
  -- 0_会員登録
  ------------------------------
  select
      c.candidate_id
    , c.ins_datetime as act_date -- 登録日
    , '0_登録' as act_type
    , case when (split_part(cpi.tracking_code,'\_',1) is null or split_part(cpi.tracking_code,'\_',1) in ('Organic','app')) 
             then 'Organic' else split_part(cpi.tracking_code,'\_',1) end as route_info
    , NULL as job_id
    , NULL as recruiter_company_id
    , NULL as message_thread_id
    , NULL as ps_reply_date
    , row_number()over(partition by to_char(c.ins_datetime,'yyyy-mm'),c.candidate_id order by c.ins_datetime) as act_no_per_month
 	FROM
		candidate c
		left join candidate_personal_info cpi on c.candidate_id = cpi.candidate_id
	WHERE
		c.ins_datetime >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))

UNION ALL

  ------------------------------
  -- 1_ログイン
  ------------------------------
  select
      candidate_id
    , act_date -- ログイン日
    , '1_ログイン' as act_type
    , NULL as route_info
    , NULL as job_id
    , NULL as recruiter_company_id
    , NULL as message_thread_id
    , NULL as ps_reply_date
    , row_number()over(partition by to_char(act_date,'yyyy-mm'),candidate_id order by act_date) as act_no_per_month
  from
    (
      select distinct
        to_date(date_trunc('day',clh.login_tm),'YYYY-MM-DD') as act_date
        , clh.cid as candidate_id
      from
        candidate_login_history clh
        left join candidate c on clh.cid = c.candidate_id
      where
        -- 登録当日のログインは除く
        c.ins_datetime::date < clh.login_tm::date
        and not exists(
        -- 退会当日のログインは除く
          select 
            *
          from 
            (
            select
              c1.candidate_id
              ,upd_datetime as lft_datetime
            from
              candidate c1
            where
              c1.candidate_status_cd = 'LFT'
            )sub
          where
            clh.cid = sub.candidate_id
            and clh.login_tm::date = sub.lft_datetime::date
        )      
        and clh.login_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
    )

UNION ALL

  ------------------------------
  -- 2_興味がある
  ------------------------------
  select
      candidate_id
    , act_date -- 興味ある押下日
    , '2_興味がある' as act_type
    , NULL as route_info
    , job_id
    , recruiter_company_id
    , NULL as message_thread_id
    , NULL as ps_reply_date
     -- 月次のユニーク累積用
    , row_number()over(partition by to_char(act_date,'yyyy-mm'),candidate_id order by act_date) as act_no_per_month
  from
    (
      select distinct
        to_date(date_trunc('day',rcd.conversion_tm),'YYYY-MM-DD') as act_date
        , rcd.candidate_id
        , rcd.job_id
        , rcd.recruiter_company_id
      from
        recruiter_correspond_data rcd
      where
        rcd.conversion_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
        and rcd.conversion_type_cd = 'FLW'
        -- ビズリーチ除外
        and rcd.recruiter_company_id <> 1
    )
  
UNION ALL

  ------------------------------
  -- 3_PS送信
  ------------------------------
  select
    -- PS件数をカウントする際は、tableau等でmessage_thread_idでdistinctかけること！
      candidate_id
    , act_date
    , '3_PS送信' as act_type -- PS送信日
    , route_info
    , job_id
    , recruiter_company_id
    , message_thread_id
    , to_date(date_trunc('day',reply_tm),'YYYY-MM-DD') as ps_reply_date
    -- 月次のユニーク累積用
    , row_number()over(partition by to_char(act_date,'yyyy-mm'),candidate_id order by act_date) as act_no_per_month
  from
    (
    select distinct
        to_date(date_trunc('day',mt.scout_sent_tm),'YYYY-MM-DD') as act_date
        , mt.message_thread_id
        , mt.candidate_id
        , case when mt.scout_s_class = 'FL' then '1_興味があるPS' else '2_その他PS' end as route_info
        , mt.attached_job_id as job_id
        , mt.recruiter_company_id
        , psrep.reply_tm
    from
        message_thread mt
        -- こっちの方がクエリが軽い。PS送信は企業側から必ず行うので、min()を使って企業側からの最初のメッセージを特定する必要はない
        inner join message m
          on mt.message_thread_id = m.message_thread_id
          and mt.scout_sent_tm = m.sent_tm
          and m.from_recruiter = 'TRUE'
          and m.sent_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-6))
        left join (
          select distinct
            mt.message_thread_id
            ,mt.scout_sent_tm
            ,mt.recruiter_company_id
            ,mt.company_type_cd
            ,rep.reply_tm
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
                  and m.sent_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-6))
                group by
                  message_thread_id
            ) rep
              on mt.message_thread_id = rep.message_thread_id
          where
            --プラスカ送信スレッドに限定する
            mt.create_trigger_type_cd = 'PSC'
            --and mt.company_type_cd = 'CRS'
            --求職者がスカウトメッセージを開封している
            and mt.cn_scout_first_view_tm is not null
        )psrep on mt.message_thread_id = psrep.message_thread_id
    where
        mt.create_trigger_type_cd = 'PSC' -- PSのみに絞る
        --and mt.company_type_cd = 'CRS'
        and mt.scout_sent_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-6))
        and mt.recruiter_company_id <> 1 
  )
    
UNION ALL

  ------------------------------
  -- 4_応募
  ------------------------------

  select
      cp.candidate_id
    , cp.ins_datetime as act_date -- 応募日
    , '4_応募' as act_type
    , cp.route_info
    , cp.job_id
    , cp.recruiter_company_id
    , NULL as message_thread_id
    , NULL as ps_reply_date
     -- 月次のユニーク累積用
    , row_number()over(partition by to_char(cp.ins_datetime,'yyyy-mm'),cp.candidate_id order by cp.ins_datetime) as act_no_per_month
  from  
    cp
  where
    cp.ins_datetime >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
      
UNION ALL

  ------------------------------
  -- 5_HS
  ------------------------------
  select
      cp.candidate_id
    , cpl.ins_datetime as act_date -- HSが出た日
    , '5_HS' as act_type
    , cp.route_info
    , cp.job_id
    , cp.recruiter_company_id
    , NULL as message_thread_id
    , NULL as ps_reply_date
    -- 月次のユニーク累積用
    , row_number()over(partition by to_char(cpl.ins_datetime,'yyyy-mm'),cp.candidate_id order by cpl.ins_datetime) as act_no_per_month
  from
    cp 
    inner join candidate_progress_log cpl on cp.candidate_progress_id = cpl.candidate_progress_id 
  where
    cpl.DEL_FLG = 'N'
    and cpl.CANDIDATE_PROGRESS_STATUS_CD in('ITV__', 'MTADJ') -- CRS:書類OK / ESS:面談調整中
    and cpl.ins_datetime >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))

UNION ALL

  ------------------------------  
  -- 6_内定
  ------------------------------
  
  select
      cp.candidate_id
    , cpl.ins_datetime as act_date -- 内定が出た日
    , '6_内定' as act_type
    , cp.route_info
    , cp.job_id
    , cp.recruiter_company_id
    , NULL as message_thread_id
    , NULL as ps_reply_date
    -- 月次のユニーク累積用    
    , row_number()over(partition by to_char(cpl.ins_datetime,'yyyy-mm'),cp.candidate_id order by cpl.ins_datetime) as act_no_per_month
  from
    cp
    inner join candidate_progress_log cpl on cp.candidate_progress_id = cpl.candidate_progress_id 
  where
    cpl.DEL_FLG = 'N'
    and cpl.CANDIDATE_PROGRESS_STATUS_CD = 'OFFER' -- 内定
    and cpl.ins_datetime >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))

UNION ALL

  ------------------------------
  -- 7_決定
  ------------------------------
  select
      cr.candidate_id
    , cr.ins_datetime as act_date -- 決定が出た日
    , '7_決定' as act_type
    , cp.route_info
    , cp.job_id
    , cp.recruiter_company_id
    , NULL as message_thread_id
    , NULL as ps_reply_date
    -- 月次のユニーク累積用
    , row_number()over(partition by to_char(cr.ins_datetime,'yyyy-mm'),cr.candidate_id order by cr.ins_datetime) as act_no_per_month    
  from
    candidate_replace cr
    inner join cp on cr.candidate_progress_id = cp.candidate_progress_id
  where
    cr.ins_datetime >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
    -- 「入社前辞退」を除外
    and not exists(
      select * 
      from candidate_progress_log cpl
      where
        cp.candidate_progress_id = cpl.candidate_progress_id
        and cpl.candidate_progress_status_cd = 'HIRNG'  
        and cpl.del_flg = 'N'
    )

UNION ALL

  ------------------------------
  -- 8_退会
  ------------------------------
  select
      c.candidate_id
    , c.upd_datetime as act_date -- おそらく退会日
    , '8_退会' as act_type
    , NULL as route_info
    , NULL as job_id
    , NULL as recruiter_company_id
    , NULL as message_thread_id
    , NULL as ps_reply_date
    -- 月次のユニーク累積用
    , row_number()over(partition by to_char(c.upd_datetime,'yyyy-mm'),c.candidate_id order by c.upd_datetime) as act_no_per_month
  FROM
    candidate c
  WHERE
    c.candidate_status_cd = 'LFT'
    and c.upd_datetime >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
),

c2 as (
  with c as (
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
  from
    c 
)

-- merge 
select
  tran.candidate_id
  ,tran.act_date
  ,tran.act_type
  ,tran.route_info
  ,tran.job_id
  ,tran.recruiter_company_id
  ,tran.message_thread_id -- スカウト通数カウント用
  ,tran.ps_reply_date
  ,tran.act_no_per_month -- 月次のユニーク累積集計用
  ,c2.c_reg_datetime
  ,c2.c_gender_cd
  ,c2.c_age
  ,c2.c_school_rank_cd
  ,c2.c_candidate_rank_cd
  ,c2.c_recent_g_job
  ,c2.c_recent_job
  ,c2.c_recent_g_industory
  ,c2.c_recent_industory
  ,c2.c_recent_income
  ,c2.c_education_bg
  ,c2.c_job_change_duration_cd
  ,c2.c_rsume_char_cnt
  ,c2.c_status_cd
  ,c2.c_trcd
  ,c2.c_area
  ,c2.c_toeic
  ,c2.c_toefl
  ,c2.c_com_exp_cnt
from
  tran
  left join c2 on tran.candidate_id = c2.c_candidate_id
