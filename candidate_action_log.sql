/* データの定義
** ビズリーチへのアクションは除く
** ESS/CRS含む
** 直近13ヶ月のアクションデータ
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
  -- 3_PS
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
          and m.sent_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
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
                  and m.sent_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
                group by
                  message_thread_id
            ) rep
              on mt.message_thread_id = rep.message_thread_id
          where
            --プラスカ送信スレッドに限定する
            mt.create_trigger_type_cd = 'PSC'
            --求職者がスカウトメッセージを開封している
            and mt.cn_scout_first_view_tm is not null
        )psrep on mt.message_thread_id = psrep.message_thread_id
    where
        mt.create_trigger_type_cd = 'PSC' -- PSのみに絞る
        and mt.scout_sent_tm >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
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
  FROM
    candidate c
  WHERE
    c.candidate_status_cd = 'LFT'
    and c.upd_datetime >= date_trunc('month',add_months(trunc(convert_timezone('JST', getdate())),-13))
)

select
  *
from
  tran
