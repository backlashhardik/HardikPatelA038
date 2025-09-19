import oautils.teradata as td
import oautils.postgres as pg
import oautils.aws.secrets_manager as sm
from oautils.helper import logger
from oautils.email_ import email_on_exception
import re
import pandas as pd
import numpy as np
# import argparse
import requests
import json
from datetime import datetime, date, timedelta
import boto3
import pytz
from functools import cache
import sys

try:
    from awsglue.utils import getResolvedOptions  # type: ignore
    is_glue_job = True
except ModuleNotFoundError:
    is_glue_job = False

logg = logger()

if is_glue_job:
    try:
        args = getResolvedOptions(
            sys.argv,
            [
                "crewai_stage_name",
                "common_bucket_name",
                "crewai_secrets_name"
            ],
        )
    except Exception:
        pass

try:
    SECRET_NAME = args["crewai_secrets_name"]
    S3_BUCKET = args["common_bucket_name"]
    STAGE_NAME = args["crewai_stage_name"]

    if "037661190395" not in S3_BUCKET:
        s3_path = f"supply/{str(date.today()+timedelta(days=1))}/"
    else:
        s3_path = "sc_availability/supply/"
except Exception:
    SECRET_NAME = "CRW/BI_PA"
    S3_BUCKET = "dl-use1-edw-037661190395-crw-bipa-crwdp-data"
    s3_path = "sc_availability/supply/"


FTPWEB_URL = "https://64bxhjxy41-vpce-044873710e5adabb9.execute-api.us-east-1.amazonaws.com/ftpweb/"
TZ = pytz.timezone('US/Eastern')

# @cache
# def get_aws_account_id() -> str:
#     return boto3.client("sts").get_caller_identity()["Account"]

# def get_secret_name() -> str:
#     try:
#         return SECRET_NAMES[get_aws_account_id()]
#     except Exception as ex:
#         logg.error(ex)
#         raise ex

# SECRET_NAME = get_secret_name()

def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    """Turns Timestamp and Date columns into strings for easier handling in JSON

    Args:
        df (pd.DataFrame): DataFrame with Timestamp and Date columns that need to be cleaned

    Returns:
        pd.DataFrame: Cleaned DataFrame
    """

    date_cols = ["availability_dt", "run_date"]
    ts_cols = [
        "start_availability_gdttm",
        "end_availability_gdttm",
        "start_availability_ldttm",
        "end_availability_ldttm",
        "start_availability_sc_gdttm",
        "end_availability_sc_gdttm",
        "start_availability_sc_ldttm",
        "end_availability_sc_ldttm",
        "sc_beg_ldttm",
        "sc_end_ldttm",
        "last_updt_gdttm",
        "plt_sc_opn_tm_dbms_gdttm",
        "sc_beg_dttm",
        "sc_end_dttm",
        "dbms_crtn_dttm",
        "awd_dttm",
        "crtn_gdttm",
        "lst_updt_gdttm"
    ]

    for col in df.columns:
        if col in date_cols:
            df[col] = df[col].astype(str).str[:10].replace("NaT", None)
        elif col in ts_cols:
            df[col] = df[col].astype(str).str[:19].replace("NaT", None)

    return df


def send_to_ftpweb(data: str, target_object: str) -> None:
    """Send the JSON data to the FTPWeb API

    Args:
        data (str): JSON data to be sent
        target_object (str): Filename _without path_ to be created in the FTPWeb S3 Bucket
    """

    remote_file = "CRW/BI_PA/report_viewer/sc_availability/" + target_object

    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket="dl-use1-edw-037661190395-oap-ftpweb", Key=remote_file, Body=data
        )
    except Exception:
        logg.info("Direct PUT to S3 unsuccessful. Attempting to use the FTPWEB API")
        api_key = sm.get_value_kr(
            secret_name=SECRET_NAME, service="s3_ftpweb", key="api_key"
        )

        url = FTPWEB_URL

        file = data
        payload = {"file": file}
        headers = {"x-api-key": api_key, "Content-Type": "application/json"}

        r = requests.post(url + remote_file, files=payload, headers=headers, timeout=30)

        r.raise_for_status()

def send_csv_to_s3(data: pd.DataFrame, out_file_name: str) -> None:
    try:
        data['crew_seat_cd'] = data['crew_seat_cd'].replace({'CA':'A','FO':'B'})
        data.columns = map(str.lower, data.columns)
        data.rename(columns={"_30_168_violation": "violation_30_168"}, inplace=True, errors='ignore')
        data.drop(columns=["expire_cnt", "grte_due_min_ct", "sc_remaining"], inplace=True,errors='ignore')
        data.fillna("", inplace=True)
        data.replace({'None': '', 'NaT': '', 'NaN': ''}, inplace=True)

        for col in data.columns:
            if str(data[col].dtypes)[:5] == "float":
                data[col] = data[col].astype(int)

        data.to_csv('temp.csv', index=False)

        for col in data.columns:
            if str(data[col].dtypes)[:5] == "float":
                data[col] = data[col].astype(int)

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=S3_BUCKET, Key=s3_path+out_file_name, Body=open('temp.csv', 'r').read()
        )
    except Exception as ex:
        logg.error(ex)
        raise ex

@logg.time_info
def tz(db_conn: td.sqlalchemy.engine.Connection, print_sql: bool = False) -> None:
    """Create a refernce table with Crew Base Timezones

    Args:
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Teradata DWPROD Database
        print_sql (bool): If True, print the SQL query instead of executing it
    """

    sql = """
        create volatile table tz (
            base char(3) not null,
            tz varchar(30),
            primary key (base)
        ) on commit preserve rows
        ;
        """

    ins_sql = """
        insert into tz values('ATL','America Eastern');
        insert into tz values('NYC','America Eastern');
        insert into tz values('DTW','America Eastern');
        insert into tz values('BOS','America Eastern');
        insert into tz values('MSP','America Central');
        insert into tz values('SLC','America Mountain');
        insert into tz values('SEA','America Pacific');
        insert into tz values('LAX','America Pacific');
        """

    if print_sql:
        print(sql)
        print(ins_sql)
    else:
        td.execute(qry_txt=sql, conn=db_conn)
        td.execute(qry_txt=ins_sql, conn=db_conn)

    return


@logg.time_info
def eas(
    temporal_qualifier: str,
    db_conn: td.sqlalchemy.engine.Connection,
    print_sql: bool = False,
) -> None:
    """Create the Employee Activity Status table (eas)

    Args:
        temporal_qualifier (str): Teradata temporal qualifier for the query
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Teradata DWPROD Database
        print_sql (bool): If True, print the SQL query instead of executing it
    """

    extract_ts = "current_timestamp"

    if temporal_qualifier != "current validtime":
        extract_ts = re.search(
            pattern=r"timestamp '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*?'",
            string=temporal_qualifier,
        ).group(0)

    tmrw_start = f"trunc({extract_ts}, 'DD') + interval '1' day"

    sql = f"""
        create volatile table eas as(
        {temporal_qualifier}
        select
        normalize
            substr(epc.plt_catg_cd, 1, 3) as base
            , substr(epc.plt_catg_cd, 4, 3) as fleet
            , substr(epc.plt_catg_cd, 7, 2) as seat
            , eas.crew_bid_perd_cd
            , eas.empl_id
            , ast.atvy_stt_cd
            , cast(to_char(eas.crtn_gdttm, 'YYYY-MM-DD HH24:MI:SS') || '.000000+00:00' as timestamp with time zone) as crtn_gdttm
            , period(
                cast(to_char(eas.Atvy_Stt_Beg_gDttm, 'YYYY-MM-DD HH24:MI:SS') || '.000000+00:00' as timestamp with time zone)
                , cast(to_char(eas.Atvy_Stt_end_gDttm, 'YYYY-MM-DD HH24:MI:SS') || '.000000+00:00' as timestamp with time zone) +
                    case extract(minute from eas.Atvy_Stt_end_gDttm)
                        when 59 then interval '1' minute
                        else interval '0' minute
                        end
                ) as stt_prd
            , case
                when atvy_stt_cd like any(
                    '**', '%VAC%', '%IVD%', 'XX', '%RST%', '%PD%', '%P/D%', 'SICK', '12N%', '_LOA', 'SUP_', '_SIK', '%NQ%', '__AS'
                    , 'PB', 'PBR', 'OJI', 'CADM' ,'CPR' ,'DIF' ,'DPMA' ,'DSCD' ,'FAAL','FMLA' ,'FULL' ,'FURL', 'FX','HURR', 'JURY'
                    , 'LFLY' ,'MLOV' ,'MLOX' ,'NFL' ,'NFLY' ,'NHXX' ,'NMOV' ,'NOE', 'NSIC' ,'PJRY' ,'PMOV' ,'RET' ,'SAQD' ,'SIL'
                    , 'SUSN' ,'SUSP' ,'TERM' ,'TOFF' ,'ULOA' , 'PFTM', 'FOSP','BMTG','PPLS' ,'CBPS', 'PVPP', 'PR', 'PNTL'
                    )
                    then 1
                else 0
                end as rst_stt_ind
        from css.empl_atvy_stt eas
            inner join css.atvy_stt ast
                ON eas.atvy_stt_id = ast.atvy_stt_id
            inner join (
                {temporal_qualifier}
                select *
                from cru.empl_plt_catg
                where {tmrw_start} between empl_plt_pstn_catg_eff_dt and exp_dt
                qualify 1 = rank() over(
                    partition by empl_id
                    order by EMPL_PLT_PSTN_CATG_EFF_DT desc, DWH_VALID_FROM_TS desc
                    )
            ) epc
                on eas.empl_id = epc.empl_id
                and cast(eas.atvy_stt_beg_gdttm as date) <= epc.exp_dt
                and cast(eas.atvy_stt_end_gdttm as date) >= epc.empl_plt_pstn_catg_eff_dt
            inner join cru.crew_bid_perd cbp
                on cast({tmrw_start} as timestamp)
                    between cbp.crew_bid_perd_strt_dt - interval '30' day and cbp.crew_bid_perd_end_dt + interval '30' day
                and cast(eas.atvy_stt_beg_gdttm as date) <= cast(cbp.crew_bid_perd_end_dt + interval '30' day as date)
                and cast(eas.atvy_stt_end_gdttm as date) >= cast(cbp.crew_bid_perd_strt_dt - interval '30' day as date)
                and cbp.empl_role_cd = 'P'
        where eas.empl_role_cd = 'P'
        and  eas.atvy_stt_atve_ind = 'Y'
        and eas.Atvy_Stt_Beg_gDttm < eas.Atvy_Stt_end_gDttm
    )
    with data
    primary index (empl_id)
    on commit preserve rows
    ;
    """  # noqa: E501

    if print_sql:
        print(sql)
    else:
        td.execute(qry_txt=sql, conn=db_conn)

    return


@logg.time_info
def rots(
    temporal_qualifier: str,
    db_conn: td.sqlalchemy.engine.Connection,
    print_sql: bool = False,
) -> None:
    """Create the rotation information table (rots)

    Args:
        temporal_qualifier (str): Teradata temporal qualifier for the query
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Teradata DWPROD Database
        print_sql (bool): If True, print the SQL query instead of executing it
    """

    extract_ts = "current_timestamp"

    if temporal_qualifier != "current validtime":
        extract_ts = re.search(
            pattern=r"timestamp '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*?'",
            string=temporal_qualifier,
        ).group(0)

    tmrw_start = f"trunc({extract_ts}, 'DD') + interval '1' day"

    sql = f"""
        create volatile table rots as(
        {temporal_qualifier}
        select
            substr(epc.plt_catg_cd, 1, 3) as base
            , substr(epc.plt_catg_cd, 4, 3) as fleet
            , substr(epc.plt_catg_cd, 7, 2) as seat
            , er.crew_bid_perd_cd
            , erl.empl_id
            , cast('xROTN' as varchar(50)) as atvy_stt_cd
            , er.rotn_prng_nb
            , er.rotn_beg_dt
            , erl.empl_rotn_duty_sq_nb
            , erl.rotn_flt_leg_sq_nb
            , coalesce(flt_leg_id, -1) as flt_leg_id
            , coalesce(flt_leg_hst_id, -1) as flt_leg_hst_id
            , coalesce(grnd_leg_id, -1) as grnd_leg_id
            , coalesce(grnd_misc_leg_id, -1) as grnd_misc_leg_id
            , min(cast(to_char(erd.sch_duty_rpt_gdttm, 'YYYY-MM-DD HH24:MI:SS') || '.000000+00:00' as timestamp with time zone)) over(partition by er.empl_rotn_id, erl.empl_id, er.crew_bid_perd_cd, er.rotn_prng_nb, er.rotn_beg_dt, erd.empl_rotn_duty_sq_nb) as bg
            , max(cast(to_char(erd.actl_duty_rls_gdttm, 'YYYY-MM-DD HH24:MI:SS') || '.000000+00:00' as timestamp with time zone) + interval '1' minute) over(partition by er.empl_rotn_id, erl.empl_id, er.crew_bid_perd_cd, er.rotn_prng_nb, er.rotn_beg_dt, erd.empl_rotn_duty_sq_nb) as en
            , period(bg, case when bg > en then en + interval '1' day else en end) as duty_stt_prd
        from css.empl_rotn er
            inner join css.empl_rotn_duty erd
                on er.empl_rotn_id = erd.empl_rotn_id
            inner join css.empl_rotn_leg erl
                on erd.empl_rotn_id = erl.empl_rotn_id
                and erd.empl_rotn_duty_sq_nb = erl.empl_rotn_duty_sq_nb
            inner join (
                {temporal_qualifier}
                select *
                from cru.empl_plt_catg
                where {tmrw_start} between empl_plt_pstn_catg_eff_dt and exp_dt
                qualify 1 = rank() over(
                    partition by empl_id
                    order by EMPL_PLT_PSTN_CATG_EFF_DT desc, DWH_VALID_FROM_TS desc
                    )
            ) epc
                on erl.empl_id = epc.empl_id
                and cast(erd.sch_duty_rpt_ldttm as date) <= epc.exp_dt
                and cast(erd.sch_duty_rls_ldttm as date) >= epc.empl_plt_pstn_catg_eff_dt
            inner join cru.crew_bid_perd cbp
                on {tmrw_start}
                    between cbp.crew_bid_perd_strt_dt - interval '45' day and cbp.crew_bid_perd_end_dt + interval '45' day
                and er.crew_bid_perd_cd = cbp.crew_bid_perd_cd
                and cbp.empl_role_cd = 'P'
        where  er.rotn_actn_cd <> 'DELETE'
        and  erl.rotn_actn_cd <> 'DELETE'
        and erl.empl_id is not null
        qualify bg < case when bg > en then en + interval '1' day else en end
        -- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    )
    with data
    primary index (empl_id, flt_leg_id, flt_leg_hst_id, grnd_leg_id, grnd_misc_leg_id)
    on commit preserve rows
    ;
    """  # noqa: E501

    if print_sql:
        print(sql)
    else:
        td.execute(qry_txt=sql, conn=db_conn)

    return


@logg.time_info
def pfs(
    temporal_qualifier: str,
    db_conn: td.sqlalchemy.engine.Connection,
    print_sql: bool = False,
) -> None:
    """Create the Pilot Full Schedule table (plt_full_sched)

    Args:
        temporal_qualifier (str): Teradata temporal qualifier for the query
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Teradata DWPROD Database
        print_sql (bool): If True, print the SQL query instead of executing it
    """

    sql = """
        create volatile table plt_full_sched as(
        select normalize
            base, fleet, seat, crew_bid_perd_cd, empl_id, cast(atvy_stt_cd as varchar(50)) as atvy_stt_cd,
            cast(null as varchar(50)) as rotn_prng_nb,
            cast(null as timestamp(6)) as rotn_beg_dt,
            stt_prd,
            rst_stt_ind
        from eas
        union all
        select
            base, fleet, seat, crew_bid_perd_cd, empl_id, atvy_stt_cd,
            rotn_prng_nb,
            rotn_beg_dt,
            period(min(begin(duty_stt_prd)), max(end(duty_stt_prd))) as stt_prd,
            0 as rst_stt_ind
        from rots
        group by 1,2,3,4,5,6,7,8,10
        union all
        select
                base, fleet, seat, crew_bid_perd_cd, empl_id, atvy_stt_cd,
                rotn_prng_nb, rotn_beg_dt,
                period(curr_actl_duty_rls_gdttm, next_actl_duty_rpt_gdttm) as stt_prd,
            1 as rst_stt_ind
    --            , interval (stt_prd) hour(4)
        from (
                select
                ers.base
                , ers.fleet
                , ers.seat
                , ers.crew_bid_perd_cd
                , ers.empl_id
                , 'xROTN_LYVRRST' as atvy_stt_cd
                , ers.rotn_prng_nb
                , ers.rotn_beg_dt
                , 1 as rst_stt_ind
                , cast(to_char(erd.actl_duty_rls_gdttm, 'YYYY-MM-DD HH24:MI:SS') || '.000000+00:00' as timestamp with time zone) as curr_actl_duty_rls_gdttm
                , erd.Duty_Perd_Lyov_Mnt_Ct
                , curr_actl_duty_rls_gdttm
                    + cast(floor(erd.Duty_Perd_Lyov_Mnt_Ct / 60) as interval hour(4))
                    + cast(erd.Duty_Perd_Lyov_Mnt_Ct mod 60 as interval minute(4))
                    as next_actl_duty_rpt_gdttm
                from css.empl_rotn er
                    inner join css.empl_rotn_duty erd
                        on er.empl_rotn_id = erd.empl_rotn_id
                    inner join (select distinct base, fleet, seat, crew_bid_perd_cd, empl_id, rotn_prng_nb, rotn_beg_dt from rots) ers
                        on ers.rotn_prng_nb = er.rotn_prng_nb
                        and ers.rotn_beg_dt = er.rotn_beg_dt
                where exists(select 1 from css.empl_rotn_leg erl where erl.empl_rotn_id = er.empl_rotn_id and erl.empl_id = ers.empl_id and erl.rotn_actn_cd not in('DELETE', 'NOOP'))
                and er.rotn_actn_cd not in('DELETE', 'NOOP')
                and erd.Duty_Perd_Lyov_Mnt_Ct > 0
            ) sub
    )
    with data
    primary index (empl_id)
    on commit preserve rows
    ;
    """  # noqa: E501

    if print_sql:
        print(sql)
    else:
        td.execute(qry_txt=sql, conn=db_conn)

    return


@logg.time_info
def ctt(
    temporal_qualifier: str,
    db_conn: td.sqlalchemy.engine.Connection,
    print_sql: bool = False,
) -> None:
    """Create the Can't Touch This table (cant_touch_this)

    Args:
        temporal_qualifier (str): Teradata temporal qualifier for the query
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Teradata DWPROD Database
        print_sql (bool): If True, print the SQL query instead of executing it
    """

    extract_ts = "current_timestamp"

    if temporal_qualifier != "current validtime":
        extract_ts = re.search(
            pattern=r"timestamp '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*?'",
            string=temporal_qualifier,
        ).group(0)

    sql = f"""
        create volatile table cant_touch_this as (
            select
                c.base,
                fleet,
                seat,
                crew_bid_perd_cd,
                empl_id,
                'zPOST_' || c.atvy_stt_cd as atvy_stt_cd,
                rotn_prng_nb,
                rotn_beg_dt,
                period(
                    end(stt_prd),
                    greatest(
                        end(stt_prd) +
                            case
                                when c.rst_stt_ind = 1
                                    and zfopm_fomt.timestamp_diff(end(stt_prd), begin(stt_prd)) >= 86399
                                    and ({extract_ts}) at time zone 'GMT' < end(stt_prd) - interval '12' hour
                                        then interval '10' hour
                                else interval '18' hour
                                end
                        , (({extract_ts}) at time zone 'GMT') + interval '18' hour
                        )
                    ) as stt_prd,
                0 as rst_stt_prd
            from (select * from plt_full_sched where atvy_stt_cd not in('LC')) c
            union all select * from plt_full_sched
    )
    with data
    primary index (empl_id)
    on commit preserve rows
    ;
    """  # noqa: E501

    if print_sql:
        print(sql)
    else:
        td.execute(qry_txt=sql, conn=db_conn)

    return


@logg.time_info
def bd(
    temporal_qualifier: str,
    db_conn: td.sqlalchemy.engine.Connection,
    print_sql: bool = False,
) -> None:
    """Create the Blank Days table (blank_days)

    Args:
        temporal_qualifier (str): Teradata temporal qualifier for the query
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Teradata DWPROD Database
        print_sql (bool): If True, print the SQL query instead of executing it
    """

    sql = """
        create volatile table blank_days as (
            with ctt as (
                select
                normalize
                    empl_id, stt_prd
                from plt_full_sched
                where atvy_stt_cd not like 'z%' (casespecific)
            )
            , neg_space as (
                select
                    empl_id,
                    end(stt_prd) prd_start,
                    lead(begin(stt_prd)) over(partition by empl_id order by begin(stt_prd)) prd_end,
                    period(prd_start, prd_end) as stt_prd
                from ctt
                qualify prd_end is not null
            )
            select
                empl_id,
                'OFLN' as atvy_stt_cd,
                timestamp '1900-01-01 00:00:00+00:00' as crtn_gdttm,
                stt_prd
            from neg_space
        )
        with data
        primary index (empl_id)
        on commit preserve rows
        ;
    """  # noqa: E501

    if print_sql:
        print(sql)
    else:
        td.execute(qry_txt=sql, conn=db_conn)

    return


@logg.time_info
def prsptv_rst(
    temporal_qualifier: str,
    db_conn: td.sqlalchemy.engine.Connection,
    print_sql: bool = False,
) -> None:
    """Create the Prospective Rest table (prsptv_rst)

    Args:
        temporal_qualifier (str): Teradata temporal qualifier for the query
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Teradata DWPROD Database
        print_sql (bool): If True, print the SQL query instead of executing it
    """

    sql = """
        create volatile table prsptv_rst as (
            with cte0 as (
                select normalize
                    empl_id,
                    crtn_gdttm,
                    stt_prd
                from eas
                where rst_stt_ind = 1
                union
                select empl_id, crtn_gdttm, stt_prd from blank_days
                union
                select normalize
                    empl_id,
                    timestamp '1900-01-01 00:00:00+00:00' as crtn_gdttm,
                    stt_prd
                from plt_full_sched
                where rst_stt_ind = 1
                and atvy_stt_cd like 'xROTN%'
            )
            , cte_big_block as (
                select normalize
                    a.empl_id,
                    a.stt_prd
                from cte0 a
            )
            , cte1 as (
                select normalize
                    a.empl_id,
                    max(c.crtn_gdttm) over(partition by a.empl_id, b.stt_prd) as crtn_gdttm,
                    c.stt_prd
                from cte_big_block a
                    inner join cte0 b
                        on a.empl_id = b.empl_id
                        and a.stt_prd contains b.stt_prd
                    inner join cte0 c
                        on a.empl_id = c.empl_id
                        and a.stt_prd contains c.stt_prd
                        and c.crtn_gdttm < begin(b.stt_prd)
                        and b.crtn_gdttm < begin(c.stt_prd)
                        and begin(c.stt_prd) >= begin(b.stt_prd)
        --            where a.empl_id = '0000034021'
        --            order by 1,begin(b.stt_prd), 2,4
            )
            , cte2 as (
                select
                    empl_id, stt_prd, max(crtn_gdttm) as crtn_gdttm
                from cte1
                group by 1,2
    --            order by 1,begin(stt_prd), 3
            )
            , cte2_1 as (
                select
                normalize
                    a.*
                from cte2 a
                where not exists (
                    select 1
                    from cte2 b
                    where a.empl_id = b.empl_id
                    and a.stt_prd <> b.stt_prd
                    and b.stt_prd contains a.stt_prd
                )
    --            order by 1,begin(stt_prd), 3
            )
            select
                c.*,
                zfopm_fomt.timestamp_diff(end(stt_prd), begin(stt_prd)) / 60 / 60 as rst_duration_hr,
                case
                    when rst_duration_hr >= 30 then
                        period(
                            begin(stt_prd) + interval '30' hour,
                            end(stt_prd) + interval '138' hour
                            )
                    end as rst_30_168_vldty
            from cte2_1 c
        )
        with data
        primary index (empl_id)
        on commit preserve rows
        -- order by 1,2, begin(stt_prd)
        ;
    """  # noqa: E501

    if print_sql:
        print(sql)
    else:
        td.execute(qry_txt=sql, conn=db_conn)

    return


@logg.time_info
def avail(
    temporal_qualifier: str,
    db_conn: td.sqlalchemy.engine.Connection,
    print_sql: bool = False,
) -> pd.DataFrame:
    """Determine Pilot Availability for next day and return it as a DataFrame

    Args:
        temporal_qualifier (str): Teradata temporal qualifier for the query
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Teradata DWPROD Database
        print_sql (bool): If True, print the SQL query instead of executing it

    Returns:
        pd.DataFrame: Pilot Availability for the next day
    """

    extract_ts = "current_timestamp"

    if temporal_qualifier != "current validtime":
        extract_ts = re.search(
            pattern=r"timestamp '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*?'",
            string=temporal_qualifier,
        ).group(0)

    free_dur = f"({extract_ts} at time zone 'America Eastern') + interval '18' hour"
    tmrw_start = f"trunc({extract_ts} at time zone tz.tz, 'DD') + interval '1' day"
    tmrw_end = f"trunc({extract_ts} at time zone tz.tz, 'DD') + interval '2' day"

    sql = f"""
        with ctt as (
            select
            normalize
                base, fleet, seat, empl_id, stt_prd
            from cant_touch_this
            where atvy_stt_cd not in('LC')
        )
        , neg_space as (
            select
                base, fleet, seat, empl_id,
                end(stt_prd) prd_start,
                lead(begin(stt_prd)) over(partition by base, fleet, seat, empl_id order by begin(stt_prd)) prd_end,
                period(prd_start, prd_end) as stt_prd
            from ctt
            qualify prd_end is not null
        )
        , lc_sc as (
            select
            normalize
                base, fleet, seat, empl_id,
                stt_prd --, crew_bid_perd_cd
            from cant_touch_this
            where atvy_stt_cd in('LC')
        )
        , sc_count as (
            select empl_id, count(*) as sc_count, max(begin(stt_prd)) as start_availability_sc_gts, max(end(stt_prd)) as end_availability_sc_gts
            from cant_touch_this c
              inner join cru.crew_bid_perd cbp
                on cast({tmrw_start.replace(" at time zone tz.tz","")} as timestamp) between cbp.crew_bid_perd_strt_dt and cbp.crew_bid_perd_end_dt
                and c.crew_bid_perd_cd = cbp.crew_bid_perd_cd
            where atvy_stt_cd = 'SC'
            and cbp.empl_role_cd = 'P'
            group by 1
        )
        , outpt as (
            select
                '{temporal_qualifier.replace("'", "''")}' as temporal_qualifier,
                cbp.crew_bid_perd_cd,
                l.base, l.fleet, l.seat, l.empl_id,
                n.stt_prd as neg_space_gts,
                l.stt_prd as lc_sc_gts,
                rst_30_168_vldty as rst_30_168_vldty_gts,
                n.stt_prd p_intersect l.stt_prd /*p_intersect rst_30_168_vldty*/ as sc_avail_prd_gts,
                period(begin(n.stt_prd) at time zone tz.tz, end(n.stt_prd) at time zone tz.tz) as neg_space_lts,
                period(begin(l.stt_prd) at time zone tz.tz, end(l.stt_prd) at time zone tz.tz) as lc_sc_lts,
                period(begin(rst_30_168_vldty) at time zone tz.tz, end(rst_30_168_vldty) at time zone tz.tz) as rst_30_168_vldty_lts,
                period(begin(sc_avail_prd_gts) at time zone tz.tz, end(sc_avail_prd_gts) at time zone tz.tz) as sc_avail_prd_lts,
                cast(to_char(greatest(begin(sc_avail_prd_gts), {free_dur}) at time zone tz.tz, 'YYYY-MM-DD') as date) as lcl_date_beg,
                cast(to_char((end(sc_avail_prd_gts) at time zone tz.tz) - interval '1' minute, 'YYYY-MM-DD') as date) as lcl_date_end,
                -- lcl_date_end - lcl_date_beg + 1 as free_duration,
                greatest(
                    1
                    , cast(to_char(end(sc_avail_prd_lts), 'YYYY-MM-DD') as date) - cast(to_char(greatest(begin(sc_avail_prd_lts), {free_dur}), 'YYYY-MM-DD') as date)
                ) as free_duration,
                6 as max_sc,
                coalesce(s.sc_count, 0) as sc_count,
                start_availability_sc_gts,
                end_availability_sc_gts,
                start_availability_sc_gts at time zone tz.tz as start_availability_sc_lts,
                end_availability_sc_gts at time zone tz.tz as end_availability_sc_lts,
                case
                    when end(rst_30_168_vldty) < end(sc_avail_prd_gts) or rst_30_168_vldty is null then 1
                    else 0
                    end as _30_168_violation
            from neg_space n
                inner join lc_sc l
                    on n.empl_id = l.empl_id
                    and n.stt_prd overlaps l.stt_prd
                inner join tz
                    on l.base = tz.base
                inner join cru.crew_bid_perd cbp
                    on cast({tmrw_start.replace(" at time zone tz.tz","")} as timestamp) between cbp.crew_bid_perd_strt_dt and cbp.crew_bid_perd_end_dt
                    and cbp.empl_role_cd = 'P'
                left join sc_count s
                    on l.empl_id = s.empl_id
                left join (select normalize empl_id, rst_30_168_vldty from prsptv_rst) pr
                    on n.empl_id = pr.empl_id
                    and n.stt_prd overlaps pr.rst_30_168_vldty
            where
                cast(greatest(begin(sc_avail_prd_gts), {free_dur}) at time zone tz.tz as date) <= {tmrw_end}
                and cast((end(sc_avail_prd_gts) at time zone tz.tz) - interval '1' minute as date) >= {tmrw_start}
        )
        select
            o.temporal_qualifier as data_as_of,
            o.crew_bid_perd_cd as "Crew_Bid_Perd_Cd",
            o.empl_id,
            'P' as empl_role_cd,
            o.seat as crew_seat_cd,
            o.base as crew_bs_cd,
            o.fleet as crew_fleet_cd,
            cast(begin(sc_avail_prd_gts) as date) - cast(coalesce(e.empl_adjd_snty_dt, e.empl_hre_dt) as date) as seniority_level,
            o.lcl_date_beg as availability_dt,
            -- begin(sc_avail_prd_gts) as start_availability_gdttm,
            -- end(sc_avail_prd_gts) as end_availability_gdttm,
            begin(sc_avail_prd_lts) as start_availability_ldttm,
            end(sc_avail_prd_lts) -
                case when extract(minute from end(sc_avail_prd_lts)) = 0 then interval '1' minute else interval '0' minute end
                as end_availability_ldttm, -- SPT wants this to be the inclusive end time
            -- start_availability_sc_gts as start_availability_sc_gdttm,
            -- end_availability_sc_gts as end_availability_sc_gdttm,
            start_availability_sc_lts as start_availability_sc_ldttm,
            end_availability_sc_lts -
                case when extract(minute from end_availability_sc_lts) = 0 then interval '1' minute else interval '0' minute end
                as end_availability_sc_ldttm, -- SPT wants this to be the inclusive end time
            free_duration as availability_length,
            max_sc as max_sc_allowed,
            sc_count as sc_scheduled,
            cast(null as int) as expire_cnt,
            _30_168_violation,
            current_timestamp AT time ZONE '+00:00' AS last_updt_gdttm
        from outpt o
            inner join cru.employee e
                on o.empl_id = e.empl_id
        where cast({tmrw_start} as date) = o.lcl_date_beg
        -- and free_duration > 0
        and zfopm_fomt.timestamp_diff(end(sc_avail_prd_gts), begin(sc_avail_prd_gts)) >= 64799 -- min 18 hours available
        qualify
            end(sc_avail_prd_gts) = min(end(sc_avail_prd_gts)) over(partition by o.empl_id)
            and _30_168_violation = min(_30_168_violation) over(partition by o.empl_id)
        order by o.base, o.fleet, o.seat, o.empl_id, begin(sc_avail_prd_gts)
        ;
        """  # noqa: E501

    if print_sql:
        print(sql)
    else:
        return td.get_data(query=sql, conn=db_conn)

    return


@logg.time_info
def max_sc(
    df: pd.DataFrame, run_date: str, db_conn: pg.sqlalchemy.engine.Connection
) -> pd.DataFrame:
    """Fetch the maximum Short Call count for each pilot and add it to the provided DataFrame

    Args:
        df (pd.DataFrame): DataFrame that was returned by the avail function
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Crew Oracle Database

    Returns:
        pd.DataFrame: Input DataFrame with the maximum Short Call and remaining Short Call counts for each pilot added
    """

    sql = f"""
        SELECT
            cbp.crew_bid_perd_cd
            , pmtd.empl_id AS empnbr
            , pmtd.mthly_sc_ct
            , pmtd.max_sc_alwd_ct
            , pmtd.max_pick_up_tm_min_ct
            , ers.empl_snty_nb
            , pmtd.grte_due as grte_due_min_ct
        FROM cru.crew_bid_perd cbp
            INNER JOIN css.plt_mthly_tm_dbms pmtd
                ON cbp.crew_bid_perd_cd = pmtd.crew_bid_perd_cd
            INNER JOIN cru.empl_role_snty ers
                ON pmtd.empl_id = ers.empl_id
                AND date '{run_date}' + interval '1' day BETWEEN ers.empl_snty_eff_dt AND ers.exp_dt
        WHERE date '{run_date}' + interval '1' day BETWEEN cbp.crew_bid_perd_strt_dt AND cbp.crew_bid_perd_end_dt
        AND cbp.empl_role_cd = 'P'
        """

    max_sc_df = pg.get_data(query=sql, conn=db_conn)

    df.drop(columns=["max_sc_allowed"], inplace=True)

    df = pd.merge(
        left=df,
        right=max_sc_df,
        how="left",
        left_on=["Crew_Bid_Perd_Cd", "Empl_Id"],
        right_on=["crew_bid_perd_cd", "empnbr"],
    )

    df["sc_scheduled"] = df["mthly_sc_ct"]

    df["max_sc_alwd_ct"] = df["max_sc_alwd_ct"].fillna(0)
    df["sc_scheduled"] = df["sc_scheduled"].fillna(0)

    df["sc_remaining"] = df["max_sc_alwd_ct"] - df["sc_scheduled"]
    df["seniority_level"] = df["empl_snty_nb"]

    df.drop(columns=["crew_bid_perd_cd", "empnbr", "empl_snty_nb", "mthly_sc_ct"], inplace=True)

    return df


@logg.time_info
def sc_prfrn(
    df: pd.DataFrame, run_date: str, db_conn: pg.sqlalchemy.engine.Connection
) -> pd.DataFrame:
    """Fetch the Short Call preferences for each pilot

    Args:
        df (pd.DataFrame): DataFrame that was returned by the avail function
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Crew Oracle Database

    Returns:
        pd.DataFrame: DataFrame with the Short Call preferences for each pilot
    """
    sql = f"""
        SELECT DISTINCT
            psrd.crew_bid_perd_cd,
            psrd.empl_id,
            cast(psrd.sc_beg_dttm as date) AS availability_dt,
            'P' as empl_role_cd,
            substr(epc.plt_catg_cd,7,2) AS crew_seat_cd,
            substr(epc.plt_catg_cd,1,3) AS crew_bs_cd,
            substr(epc.plt_catg_cd,4,3) AS crew_fleet_cd,
            psrd.sc_beg_dttm as sc_beg_ldttm,
            psrd.sc_end_dttm as sc_end_ldttm,
            psrd.pfrn_nb,
            CASE psrd.rec_typ_cd WHEN 'V' THEN 'if_needed' ELSE 'yellow_slip' END AS pref_type,
            psrd.exp_ct,
            current_timestamp at time ZONE '+00:00' AS last_updt_gdttm
        FROM css.plt_sc_req_dbms psrd
            INNER JOIN cru.empl_plt_catg epc
                ON psrd.empl_id = epc.empl_id
                AND psrd.sc_beg_dttm BETWEEN epc.empl_plt_pstn_catg_eff_dt AND epc.exp_dt
            -- INNER JOIN cru.employee e
            --     ON psrd.empl_id = e.empl_id
        WHERE psrd.sc_beg_dttm < timestamp '{run_date} 23:59:59.000' + interval '1' day
        AND psrd.sc_end_dttm > timestamp '{run_date} 00:00:00.000' + interval '1' day
        AND psrd.rec_typ_cd in('T','V')
        ORDER BY 1,2,3,4
    """

    prfrn_df = pg.get_data(query=sql, conn=db_conn)

    prfrn_df = prfrn_df[prfrn_df["empl_id"].isin(df["Empl_Id"])]

    return prfrn_df

@logg.time_info
def sc_opn_tm(
    run_date: str, db_conn: pg.sqlalchemy.engine.Connection
) -> pd.DataFrame:
    """Fetch the Short Call Open Time data

    Args:
        db_conn (td.sqlalchemy.engine.Connection): Connection to the Crew Oracle Database

    Returns:
        pd.DataFrame: DataFrame with the Short Call open time"""

    sql = f"""
        select
            date '{run_date}' as run_date
            , psotd.plt_sc_opn_tm_dbms_id
            , psotd.plt_sc_opn_tm_dbms_gdttm
            , psotd.dbms_int_sq_nb
            , psotd.rec_stt
            , 'P' as empl_role_cd
            , substr(psotd.plt_catg_cd,7,2) AS crew_seat_cd
            , substr(psotd.plt_catg_cd,1,3) AS crew_bs_cd
            , substr(psotd.plt_catg_cd,4,3) AS crew_fleet_cd
            , psotd.sc_beg_dttm
            , psotd.sc_end_dttm
            , psotd.trgtd_dys_avbl
            , psotd.invs_dys_avbl
            , psotd.pri
            , psotd.frms_ind
            , psotd.dbms_crtn_dttm
            , psotd.dbms_crtn_usr_id
            , psotd.empl_id
            , psotd.empl_dys_avbl
            , psotd.awd_dttm
            , psotd.awd_usr_id
            , psotd.snty_insrt
            , psotd.pfrn_nb
            , psotd.pfrn_ind
            , psotd.prcdg_rst_mnt_ct
            , psotd.dy_aftr_off_dy
            , psotd.hst_typ
            , psotd.crtn_gdttm
            , psotd.lst_updt_gdttm
            , case
                when eas.empl_id is not null then 1
                else 0
                end as sc_still_assigned
        from css.plt_sc_opn_tm_dbms psotd
            LEFT JOIN (
                SELECT eas.empl_id, as2.atvy_stt_cd, eas.atvy_stt_beg_ldttm, eas.atvy_stt_end_ldttm
                FROM css.empl_atvy_stt eas
                    INNER JOIN css.atvy_stt as2
                        ON eas.atvy_stt_id = as2.atvy_stt_id
                WHERE eas.atvy_stt_atve_ind = 'Y'
                AND eas.empl_role_cd = 'P'
                AND as2.atvy_stt_cd = 'SC'
            ) eas
                ON psotd.empl_id = eas.empl_id
                AND eas.atvy_stt_beg_ldttm BETWEEN psotd.sc_beg_dttm - interval '2' hour AND psotd.sc_end_dttm + INTERVAL '2' hour
        where cast(psotd.sc_beg_dttm as date) between date '{run_date}' and date '{run_date}' + interval '3' day
        and coalesce(psotd.hst_typ, '.') not in('E', 'D')
        and (
            eas.empl_id is not null  -- pilot is still assigned to SC
            or psotd.rec_stt = 'O'  -- SC is still in Open Time
        )
        order by psotd.plt_sc_opn_tm_dbms_gdttm
    """

    sc_opn_tm_df = pg.get_data(query=sql, conn=db_conn)

    return sc_opn_tm_df

@logg.time_info
@email_on_exception(
    sender_email="nathan.bily@delta.com",
    dist_list=["nathan.bily@delta.com"],
    description="SC Availability has failed. :-(",
)
def main(print_sql: bool = False) -> None:
    """Run the Short Call Availability process

    Args:
        print_sql (bool, optional): Prints SQL Queries instead of running against the Database. Defaults to False.
    """

    run_dates = [str(date.today())]
    # run_dates = ["2025-03-07"]

    # Get today's date
    # today = date.today()
    # # today = date(today.year, 2, 28)

    # # Get the first day of the year
    # start_date = date(today.year, 3, 1) - timedelta(days=1)

    # # Create a list of dates from the beginning of the year to today
    # run_dates = [
    #     str(start_date + timedelta(days=x))
    #     for x in range((today - start_date).days + 1)
    # ]

    outdf = pd.DataFrame()
    outprfrn_df = pd.DataFrame()

    pg_username, pg_password = sm.get_value_kr(
        secret_name=SECRET_NAME,
        service="bipa_crwdp_sc_postgres",
        key="username",
        key2="password",
    )

    pg_params = {
        "host": "apg-enddlmendl-p01-ro.opsdata-prd.aws.delta.com",
        "port": 5432,
        "database": "apgodlcdcopsp01",
        "username": pg_username,
        "password": pg_password,
    }

    for d in run_dates:

        td_user, td_pass = sm.get_value_kr(
            secret_name=SECRET_NAME,
            service="bipa_crwdp_sc_teradata",
            key="username",
            key2="password",
        )

        dwprod_conn = td.get_conn(username=td_user, password=td_pass)

        # run_hour = datetime.now(tz=TZ).hour()

        t = f"validtime as of timestamp '{d} 11:00:00' at time zone 'America Eastern'"

        logg.info(f"Temporal Qualifier: {t}")

        tz(db_conn=dwprod_conn, print_sql=print_sql)
        eas(temporal_qualifier=t, db_conn=dwprod_conn, print_sql=print_sql)
        rots(temporal_qualifier=t, db_conn=dwprod_conn, print_sql=print_sql)
        pfs(temporal_qualifier=t, db_conn=dwprod_conn, print_sql=print_sql)
        ctt(temporal_qualifier=t, db_conn=dwprod_conn, print_sql=print_sql)
        bd(temporal_qualifier=t, db_conn=dwprod_conn, print_sql=print_sql)
        prsptv_rst(temporal_qualifier=t, db_conn=dwprod_conn, print_sql=print_sql)
        df = avail(
            temporal_qualifier=t, db_conn=dwprod_conn, print_sql=print_sql
        ).drop_duplicates(ignore_index=True)

        dwprod_conn.close()

        pg_conn = pg.get_conn(**pg_params)

        df = max_sc(df=df, run_date=d, db_conn=pg_conn).drop_duplicates(
            ignore_index=True
        )
        prfrn_df = sc_prfrn(df=df, run_date=d, db_conn=pg_conn)

        sc_opn_tm_df = sc_opn_tm(run_date=d, db_conn=pg_conn)

        pg_conn.close()

        if print_sql is False:
            if len(outdf.index) == 0:
                outdf = df.copy()
                outprfrn_df = prfrn_df.copy()
            else:
                outdf = pd.concat(objs=[outdf, df])
                outprfrn_df = pd.concat(objs=[outprfrn_df, prfrn_df])

    if print_sql is False:
        # outdf.to_csv("sc_availability/sc_availability.csv")
        # outprfrn_df.to_csv("sc_availability/sc_preferences.csv")

        if "037661190395" in S3_BUCKET:
            td.insert(
                data=outdf.replace({np.nan: None}),
                table_name="zfopm_fomt.sc_availability_hist",
                conn=td.get_conn(
                    username=td_user,
                    password=td_pass,
                ),
                delete_query=f"""delete from zfopm_fomt.sc_availability_hist
                                where availability_dt in(
                                    {"date '" + "', date '".join([str(datetime.strptime(x , '%Y-%m-%d') + timedelta(days=1))[:10] for x in run_dates]) + "'"}
                                )""",
            )

            td.insert(
                data=clean_values(outprfrn_df).replace({np.nan: None}),
                table_name="zfopm_fomt.sc_preferences_hist",
                conn=td.get_conn(
                    username=td_user,
                    password=td_pass,
                ),
                delete_query=f"""delete from zfopm_fomt.sc_preferences_hist
                                where availability_dt in(
                                    {"date '" + "', date '".join([str(datetime.strptime(x , '%Y-%m-%d') + timedelta(days=1))[:10] for x in run_dates]) + "'"}
                                )""",
            )

            td.insert(
                data=clean_values(sc_opn_tm_df).replace({np.nan: None}),
                table_name="zfopm_fomt.sc_opn_tm_hist",
                conn=td.get_conn(
                    username=td_user,
                    password=td_pass,
                ),
                delete_query=f"""delete from zfopm_fomt.sc_opn_tm_hist
                                where run_date in(
                                    {"date '" + "', date '".join([x for x in run_dates]) + "'"}
                                )""",
            )

            json_out = json.dumps(
                {
                    "title": "Short Call Availability",
                    "lastModified": str(datetime.now())[:19],
                    "columns": [
                        "Data As Of",
                        "Bid Perd Cd",
                        "Empl ID",
                        "Empl Role",
                        "Seat",
                        "Base",
                        "Fleet",
                        "Seniority",
                        "Available Date",
                        # "Start Availability (GMT)",
                        # "End Availability (GMT)",
                        "Start Availability (Lcl)",
                        "End Availability (Lcl)",
                        # "Start Availability SC (GMT)",
                        # "End Availability SC (GMT)",
                        "Start Availability SC (Lcl)",
                        "End Availability SC (Lcl)",
                        "Availability Length",
                        "SC Scheduled",
                        "Expire Count",
                        "30/168 Violation",
                        "Last Update (GMT)",
                        "Max SC Allowed",
                        "Pick Up Limit",
                        "Guarantee Due",
                        "SC Remaining",
                    ],
                    "values": json.loads(clean_values(df).to_json(orient="records")),
                }
            )

            json_out_prfrn = json.dumps(
                {
                    "title": "Short Call Preferences",
                    "lastModified": str(datetime.now())[:19],
                    "columns": [
                        "Crew Bid Perd Cd",
                        "Empl ID",
                        "Availability Date",
                        "Empl Role",
                        "Seat",
                        "Base",
                        "Fleet",
                        "Start Availability",
                        "End Availability",
                        "Preference Number",
                        "If Needed",
                        "Expire Count",
                        "Last Update",
                    ],
                    "values": json.loads(clean_values(prfrn_df).to_json(orient="records")),
                }
            )

            json_out_sc_opn_tm = json.dumps(
                {
                    "title": "Short Call Open Time",
                    "lastModified": str(datetime.now())[:19],
                    "columns": [
                        "Run Date",
                        "plt_sc_opn_tm_dbms_id",
                        "plt_sc_opn_tm_dbms_gdttm",
                        'dbms_int_sq_nb',
                        'rec_stt',
                        "Empl Role",
                        "Seat",
                        "Base",
                        "Fleet",
                        'sc_beg_dttm',
                        'sc_end_dttm',
                        'trgtd_dys_avbl',
                        'invs_dys_avbl',
                        'pri',
                        'frms_ind',
                        'dbms_crtn_dttm',
                        'dbms_crtn_usr_id',
                        'empl_id',
                        'empl_dys_avbl',
                        'awd_dttm',
                        'awd_usr_id',
                        'snty_insrt',
                        'pfrn_nb',
                        'pfrn_ind',
                        'prcdg_rst_mnt_ct',
                        'dy_aftr_off_dy',
                        'hst_typ',
                        'crtn_gdttm',
                        'lst_updt_gdttm',
                        'sc_still_assigned',
                    ],
                    "values": json.loads(clean_values(sc_opn_tm_df).to_json(orient="records")),
                }
            )

            send_to_ftpweb(data=json_out, target_object="sc_availability.json")
            send_to_ftpweb(data=json_out_prfrn, target_object="sc_preferences.json")
            send_to_ftpweb(data=json_out_sc_opn_tm, target_object="sc_opn_tm.json")



        send_csv_to_s3(data=clean_values(df), out_file_name="pilot-reserves/reserve-availability.csv")
        send_csv_to_s3(data=clean_values(prfrn_df), out_file_name="pilot-preferences/reserve-preference.csv")
        send_csv_to_s3(data=clean_values(sc_opn_tm_df), out_file_name="early-posted-demand/sc-opn-tm.csv")

    return


if __name__ == "__main__":

    # parser = argparse.ArgumentParser(
    #     prog="Short Call Availability",
    #     description="Creates a file with the availability period for pilots elligible for Short Call",
    # )

    # parser.add_argument(
    #     "--print_sql",
    #     action="store_true",
    #     help="Do not create the file, just print the SQL statements",
    # )

    # args = parser.parse_args()

    try:
        main(print_sql=False)
    except Exception as ex:
        logg.error(str(ex))
        raise ex
