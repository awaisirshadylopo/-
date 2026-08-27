### mls inactive mapping function

import json
import boto3
import pandas as pd
import requests
import psycopg2
from psycopg2 import extras
from datetime import datetime
import os
import io
import sys
from io import StringIO
from psycopg2.extras import execute_values
import numpy as np
# from helper import LogData, LogMessage, log_message
import traceback
from itertools import chain
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def fetch_secrets(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret
    
def setup_db_connection(secret):
    db_user = secret['username']
    db_password = secret['password']
    db_host = secret['host']
    db_port = secret['port']
    db_name = secret['dbname']
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    return conn
    
def etl_direct_idx_missing_delete_listings(source_type,source_id,batch_id,listing_cursor,listing_conn,cursor,connection):
    
    deletion = """delete from stage.etl_direct_idx_missing_delete_listings where source_id = {0}""".format(source_id)
    listing_cursor.execute(deletion)
    listing_conn.commit()
    
    listing_query = '''
    select
    distinct on (source_listing_id)
    l.id as target_listing_id,
    l.source_listing_id, 
    '{0}' as source_id,
    '{1}' as batch_id,
    i.status as listing_status
         
    from stage.direct_idx_id i
    right join listing l
    on l.source_listing_id= i.source_listing_id
    and l.source_id = i.source_id
    where l.source_id = {0}
    and i.source_listing_id is null 
    and l.source_status='ACTIVE'
    order by l.source_listing_id
    '''.format(source_id, batch_id)
    
    listing_cursor.execute(listing_query)
    result = listing_cursor.fetchall()
    column_names = [desc[0] for desc in listing_cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ','.join(list(df.columns))

        
    insert_query = '''
    INSERT INTO  stage.etl_direct_idx_missing_delete_listings ({}) VALUES %s
    '''.format(cols)
    extras.execute_values(listing_cursor, insert_query, result)

    insert_Etl_Action_query = '''
    INSERT INTO idx_listing_etl_action_pool (source_id, batch_id, source_listing_id, listing_id, creation_time, action_type)
    VALUES %s
    '''

    # Prepare the data for the insert
    etl_action_data = [
        (source_id, row.batch_id, row.source_listing_id, row.target_listing_id, pd.to_datetime('now', utc=True), 'INACTIVE') 
        for row in df.itertuples(index=False)
    ]

    # Execute the insert query using execute_values
    extras.execute_values(listing_cursor, insert_Etl_Action_query, etl_action_data)
    # connection.commit()
    listing_conn.commit()
    # Upload INACTIVE actions to S3 (append to existing file)
    if not df.empty:
        # Build DataFrame matching idx_listing_etl_action_pool schema
        inactive_actions_df = pd.DataFrame(etl_action_data, columns=[
            'source_id', 'batch_id', 'source_listing_id', 'listing_id', 'creation_time', 'action_type'
        ])
        
        log_msg = {
            "Status": "Appending INACTIVE actions to S3 parquet",
            "inactive_count": len(inactive_actions_df),
        }
        print(json.dumps(log_msg))
        
        # append_to_existing_parquet(
        #     new_df=inactive_actions_df,
        #     source_id=source_id,
        #     source_type=source_type,
        #     source_name=source_name,
        #     batch_id=batch_id,
        #     class_Name="etl_action_pool",
        # )
    else:
        log_msg = {"Status": "No INACTIVE listings to append"}
        print(json.dumps(log_msg))

def threshold_calculation(source_id,batch_id,listing_cursor,listing_conn):
    
    threshold = '''
    SELECT 
    ((a.etl_counts::numeric / b.total_listings::numeric) * 100)::numeric(18,5) as inactive_threshold from (
    (select count(1) as etl_counts, source_id, {1} as batch_id from stage.etl_direct_idx_missing_delete_listings 
    where source_id = {0}
    group by 2,3) a join
    (select count(1) as total_listings, source_id from listing 
    where source_status = 'ACTIVE' 
    and source_id = {0}
    group by source_id) b 
    on a.source_id=b.source_id)
    '''.format(source_id,batch_id)
    
    listing_cursor.execute(threshold)
    result = listing_cursor.fetchone()
    if result is None:
        return result
    else:
        threshold_value = float(result[0])
        return threshold_value
    
def listing_change_insert(source_id,batch_id,listing_cursor,listing_conn):
    lc = """
    select 'STATUS_CHANGE' as change_type,
    coalesce(e.listing_status, 'Off Market') as new_value,
    ls.status as old_value,
    l.id as listing_id  , 
    e.batch_id
    from stage.etl_direct_idx_missing_delete_listings e
    join listing l 
    on e.target_listing_id = l.id
    and e.source_id = l.source_id
    join listing_status ls 
    on ls.id = l.listing_status_id
    where e.source_id = {0} 
    and e.batch_id = {1}  
    and coalesce(e.listing_status, 'Off Market') != trim(ls.status)
    """.format(source_id,batch_id)
    
    listing_cursor.execute(lc)
    result = listing_cursor.fetchall()
    column_names = [desc[0] for desc in listing_cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ','.join(list(df.columns))
    insert_query = '''
        INSERT INTO public.listing_change ({}) VALUES %s
        '''.format(cols)
    extras.execute_values(listing_cursor, insert_query, result)
    listing_conn.commit()

def in_active_marking(source_id,batch_id,listing_cursor,listing_conn):
    
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    update_1 = """
    update public.listing_change l set source_status='INACTIVE'
    from stage.etl_direct_idx_missing_delete_listings t
    where l.listing_id = t.target_listing_id
    and t.source_id= {0};
    """.format(source_id)
    listing_cursor.execute(update_1)
    listing_conn.commit()
    
    update_2 = """
    update public.listing_address_standard las set source_status='INACTIVE'
    where las.listing_address_id in (select l.id from listing_address l join stage.etl_direct_idx_missing_delete_listings t
    on l.listing_id = t.target_listing_id
    where t.batch_id = {1}
    and t.source_id = {0});
    """.format(source_id,batch_id)
    listing_cursor.execute(update_2)
    listing_conn.commit()
    
    update_3 = """
    update public.listing_address l set source_status='INACTIVE', batch_id = '{1}'
    from stage.etl_direct_idx_missing_delete_listings t
    where l.listing_id = t.target_listing_id
    and t.batch_id = {1}
    and t.source_id = {0};
    """.format(source_id,batch_id)
    listing_cursor.execute(update_3)
    listing_conn.commit()
    
    update_4 = """
    update public.listing l set source_status='INACTIVE',
    inactive_date = '{2}',
    batch_id = {1},
    y_last_update_date = '{2}',
    listing_status_id = '1',
    source_last_update_date = '{2}'
    from stage.etl_direct_idx_missing_delete_listings t
    where l.id = t.target_listing_id
    and t.source_id = {0};
    """.format(source_id,batch_id,formatted_datetime)
    listing_cursor.execute(update_4)
    listing_conn.commit()
    
def inactive_status_marking(source_id,batch_id,listing_cursor,listing_conn):
    
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    query_1 = """
	update etl_status s 
	set missing_listings = a.cdel 
	from (select count(1) as cdel from stage.etl_direct_idx_missing_delete_listings 
	where batch_id  = {0}) a
	where s.batch_id = {0};
    """.format(batch_id)
    listing_cursor.execute(query_1)
    listing_conn.commit()
    
    query_2 = """
	update source set last_refresh_date_inactive = '{1}' where id = {0};
    """.format(source_id,formatted_datetime)
    listing_cursor.execute(query_2)
    listing_conn.commit()

def etl_direct_idx_missing_status_delete_listings(source_id,batch_id,listing_cursor,listing_conn):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    query_3 = """
    Delete from stage.etl_direct_idx_missing_status_delete_listings  where source_id = {0};
    """.format(source_id)
    listing_cursor.execute(query_3)
    listing_conn.commit()
    
    sdl = """
    select 
    distinct l.id as target_listing_id,
    l.source_listing_id,
    s.source_id,
    s.batch_id,
    s.status as listing_status
    from public.listing l
    join stage.direct_idx_id s
    on l.source_listing_id = s.source_listing_id 
    and s.source_id = {0} 
    join listing_status sm
    on sm.status = s.status
    and sm.source_id=s.source_id
    where
    l.source_id = {0} 
    and sm.display_flag is false
    and l.source_status='ACTIVE'
    and upper(sm.ylopo_status) != 'SOLD'
    
    UNION
    
    select 
    distinct l.id as target_listing_id,
    l.source_listing_id,
    s.source_id,
    s.batch_id,
    s.status as listing_status
    from public.listing l
    join stage.direct_idx_id s
    on l.source_listing_id = s.source_listing_id 
    and s.source_id = {0} 
    join listing_status sm
    on sm.status = s.status
    and sm.source_id=s.source_id
    where
    l.source_id = {0} 
    and sm.display_flag is true
    and l.source_status='INACTIVE'
    """.format(source_id)
    
    listing_cursor.execute(sdl)
    result = listing_cursor.fetchall()
    column_names = [desc[0] for desc in listing_cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ','.join(list(df.columns))
    insert_query = '''
        INSERT INTO stage.etl_direct_idx_missing_status_delete_listings ({}) VALUES %s
        '''.format(cols)
    extras.execute_values(listing_cursor, insert_query, result)
    listing_conn.commit()
    
    lc = """
    select 
    'STATUS_CHANGE' as change_type,
    coalesce(e.listing_status, 'Off Market') as new_value,
    ls.status as old_value,
    l.id as listing_id  ,
    e.batch_id
    from stage.etl_direct_idx_missing_status_delete_listings e
    join listing l 
    on e.target_listing_id = l.id
    and e.source_id = l.source_id
    join listing_status ls 
    on ls.id = l.listing_status_id
    where e.source_id = {0} 
    and e.batch_id = {1}  
    and coalesce(e.listing_status, 'Off Market') != trim(ls.status)
    """.format(source_id,batch_id)
    
    listing_cursor.execute(lc)
    result = listing_cursor.fetchall()
    column_names = [desc[0] for desc in listing_cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ','.join(list(df.columns))
    insert_query = '''
        INSERT INTO public.listing_change ({}) VALUES %s
        '''.format(cols)
    extras.execute_values(listing_cursor, insert_query, result)
    listing_conn.commit()
    
    update_1 = """
    update public.listing_change lc set source_status='INACTIVE'
    from stage.etl_direct_idx_missing_status_delete_listings t
    join listing l
    on t.target_listing_id = l.id
    and t.source_id=l.source_id
    join listing_status sm
    on sm.status = t.listing_status
    and sm.source_id=t.source_id
    where lc.listing_id = t.target_listing_id
    and t.source_id={0}
    and sm.display_flag is false and l.source_status = 'ACTIVE';
    """.format(source_id)
    listing_cursor.execute(update_1)
    listing_conn.commit()
    
    update_2 = """
    update public.listing_address_standard las set source_status='INACTIVE'
    where las.listing_address_id in (select la.id from listing_address la join stage.etl_direct_idx_missing_status_delete_listings t
    on la.listing_id = t.target_listing_id
    and la.source_id=t.source_id
    join listing l
    on t.target_listing_id = l.id
    and t.source_id=l.source_id
    join listing_status sm
    on sm.status = t.listing_status
    and sm.source_id=t.source_id
    where t.batch_id = {1}
    and t.source_id = {0}
    and sm.display_flag is false and l.source_status = 'ACTIVE');
    """.format(source_id,batch_id)
    listing_cursor.execute(update_2)
    listing_conn.commit()
    
    update_3 = """
    update public.listing_address la set source_status='INACTIVE', batch_id = {1}
    from stage.etl_direct_idx_missing_status_delete_listings t
    join listing l
    on t.target_listing_id = l.id
    and t.source_id=l.source_id
    join listing_status sm
    on sm.status = t.listing_status
    and sm.source_id=t.source_id
    where la.listing_id = t.target_listing_id
    and t.batch_id = {1}
    and t.source_id = {0}
    and sm.display_flag is false and l.source_status = 'ACTIVE';
    """.format(source_id,batch_id)
    listing_cursor.execute(update_3)
    listing_conn.commit()
    
    update_4 = """	
    update public.listing l set source_status='INACTIVE'
    from stage.etl_direct_idx_missing_status_delete_listings t
    join listing_status sm
    on sm.status = t.listing_status
    and sm.source_id=t.source_id
    where l.id = t.target_listing_id
    and l.source_id = t.source_id
    and t.source_id={0}
    and sm.display_flag is false and l.source_status = 'ACTIVE';
    """.format(source_id)
    listing_cursor.execute(update_4)
    listing_conn.commit()
    
    update_5 = """	
    update public.listing_change lc set source_status='ACTIVE'
    from stage.etl_direct_idx_missing_status_delete_listings t
    join listing l
    on t.target_listing_id = l.id
    and t.source_id=l.source_id
    join listing_status sm
    on sm.status = t.listing_status
    and sm.source_id=t.source_id
    where lc.listing_id = t.target_listing_id
    and t.source_id={0}
    and sm.display_flag is True and l.source_status = 'INACTIVE';
    """.format(source_id)
    listing_cursor.execute(update_5)
    listing_conn.commit()
    
    update_6 = """	
    update public.listing_address_standard las set source_status='ACTIVE'
    where las.listing_address_id in (select la.id from listing_address la join stage.etl_direct_idx_missing_status_delete_listings t
    on la.listing_id = t.target_listing_id
    and la.source_id=t.source_id
    join listing l
    on t.target_listing_id = l.id
    and t.source_id=l.source_id
    join listing_status sm
    on sm.status = t.listing_status
    and sm.source_id=t.source_id
    where t.batch_id = {1}
    and t.source_id = {0}
    and sm.display_flag is true and l.source_status = 'INACTIVE');
    """.format(source_id,batch_id)
    listing_cursor.execute(update_6)
    listing_conn.commit()
    
    update_7 = """	
    update public.listing_address la set source_status='ACTIVE', batch_id = {1}
    from stage.etl_direct_idx_missing_status_delete_listings t
    join listing l
    on t.target_listing_id = l.id
    and t.source_id=l.source_id
    join listing_status sm
    on sm.status = t.listing_status
    and sm.source_id=t.source_id
    where la.listing_id = t.target_listing_id
    and t.batch_id = {1}
    and t.source_id = {0}
    and sm.display_flag is true and l.source_status = 'INACTIVE';
    """.format(source_id,batch_id)
    listing_cursor.execute(update_7)
    listing_conn.commit()
    
    update_8 = """	
    update public.listing l set source_status='ACTIVE'
    from stage.etl_direct_idx_missing_status_delete_listings t
    join listing_status sm
    on sm.status = t.listing_status
    and sm.source_id=t.source_id
    where l.id = t.target_listing_id
    and l.source_id = t.source_id
    and t.source_id={0}
    and sm.display_flag is true and l.source_status = 'INACTIVE';
    """.format(source_id)
    listing_cursor.execute(update_8)
    listing_conn.commit()
    
    update_9 = """
    
    update public.listing l set
    inactive_date = '{2}',
    batch_id = {1},
    y_last_update_date = '{2}',
    listing_status_id = '1',
    source_last_update_date = '{2}'
    where l.id in
    (
    select l.id
    from listing L join stage.etl_direct_idx_missing_status_delete_listings t 
    on L.id=t.target_listing_id
    and l.source_id= t.source_id
    join listing_status ls
    on ls.status = t.listing_status
    and ls.source_id= t.source_id
    where ls.display_flag is false 
    and t.source_id = {0}
    )""".format(source_id,batch_id,formatted_datetime)
    listing_cursor.execute(update_9)
    listing_conn.commit()
    
    update_10 = """
    update public.listing l set
    batch_id = {1},
    y_last_update_date = '{2}',
    listing_status_id = '1'
    where l.id in
    (
    select l.id
    from listing L join stage.etl_direct_idx_missing_status_delete_listings t 
    on L.id=t.target_listing_id
    and l.source_id= t.source_id
    join listing_status ls
    on ls.status = t.listing_status
    and ls.source_id= t.source_id
    where ls.display_flag is true 
    and t.source_id = {0}
    )""".format(source_id,batch_id,formatted_datetime)
    listing_cursor.execute(update_10)
    listing_conn.commit()
    
    
def listing_status_update(source_id,batch_id,listing_cursor,listing_conn): 
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    query_1 = """
    Delete from stage.temp_update_idx_status where source_id = {0};
    """.format(source_id)
    listing_cursor.execute(query_1)
    listing_conn.commit()
    
    sdl = """
    select 
    l.id as listing_id,
    l.source_id,
    l.source_listing_id,
    ls.status as old_status,
    d.status as new_status
    from stage.direct_idx_id d
    join listing l on l.SOURCE_LISTING_ID=d.SOURCE_LISTING_ID  AND l.source_id=d.source_id
    join listing_status ls on l.listing_status_id=ls.id 
    WHERE d.status<>ls.status 
    --and upper(d.status)<>'ACTIVE'
    and l.source_id = {0} and l.source_id not in 
    (761,676, 704 ,663, 429, 671,330, 217,506, 652,330, 678,338,715,660,328,400,705);
    
    """.format(source_id)
    listing_cursor.execute(sdl)
    result = listing_cursor.fetchall()
    column_names = [desc[0] for desc in listing_cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ','.join(list(df.columns))
    insert_query = '''
        INSERT INTO stage.temp_update_idx_status ({}) VALUES %s
        '''.format(cols)
    extras.execute_values(listing_cursor, insert_query, result)
    listing_conn.commit()
    
    
    ul = """
    select ls.id as listing_status_id,
    us.new_status,
    us.source_listing_id,
    us.source_id,
    us.listing_id 
    FROM stage.temp_update_idx_status us
    JOIN listing_status ls on ls.status = us.new_status AND ls.source_id=us.source_id
    WHERE us.source_id = {0};
    
    """.format(source_id)
    listing_cursor.execute(ul)
    result = listing_cursor.fetchall()
    
    update_query = f"""
    UPDATE public.listing AS pl
    SET listing_status_id = data.listing_status_id
    
    FROM (VALUES %s) AS data (listing_status_id,new_status,source_listing_id,source_id,listing_id)
    WHERE pl.id = data.listing_id
    and pl.source_id = data.source_id
    """
    extras.execute_values(listing_cursor, update_query, result)
    listing_conn.commit()

   
    sdl = """
    select 'STATUS_CHANGE' as change_type,
    e.new_status as new_value, 
    e.old_status as old_value,
    e.listing_id as listing_id,
    {1} as batch_id,
   -- e.source_id,
    ls.ylopo_status
    
    from stage.temp_update_idx_status e
    join listing_change lc
    on lc.listing_Id = e.listing_id
    --and lc.change_type = e.change_type
    --and lc.batch_id = e.batch_id
    join listing_status ls on 
    ls.status = e.new_status
    and ls.source_id = e.source_id
    where e.source_id = {0}
    and lc.id is not null
    group by 1,2,3,4,5,6;
       """.format(source_id,batch_id)
       
    listing_cursor.execute(sdl)
    result = listing_cursor.fetchall()
    column_names = [desc[0] for desc in listing_cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ','.join(list(df.columns))
    insert_query = '''
        INSERT INTO public.listing_change ({}) VALUES %s
        '''.format(cols)
    extras.execute_values(listing_cursor, insert_query, result)
    listing_conn.commit()
    
    
    query_2 = """
    update public.listing l set source_status='ACTIVE',inactive_date=null,batch_id = {1},sold_date=null,sold_price=null
    from stage.temp_update_idx_status t
    where l.id = t.listing_id 
    and l.listing_status_id in (select id from listing_status where source_id = {0} and display_flag is true and upper(ylopo_status) != 'SOLD')
    and t.source_id = {0}
    and l.source_status !='ACTIVE';
    	""".format(source_id,batch_id)
    listing_cursor.execute(query_2)
    listing_conn.commit()
    
    
    query_3 = """
    update public.listing_address l set source_status='ACTIVE',batch_id = {1}
    from  stage.temp_update_idx_status t 
    join listing li
    on li.id=t.listing_id
    join listing_status ls
    on li.listing_status_id=ls.id
    where l.listing_id = t.listing_id
    and ls.display_flag is true
    and upper(ls.ylopo_status) != 'SOLD'
    and t.source_id = {0}
    and l.source_status != 'ACTIVE';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_3)
    listing_conn.commit()
    
    
    query_4 = """
    update public.listing_address_standard las set source_status='ACTIVE',batch_id = {1}
    where las.listing_address_id in (select la.id from listing_address la 
    join stage.temp_update_idx_status t
    on t.listing_id=la.listing_id
    join listing l 
    on l.id=la.listing_id  
    join listing_status ls
    on l.listing_status_id=ls.id
    where ls.display_flag is true
    and t.source_id = {0}
    and upper(ls.ylopo_status) != 'SOLD')
    and las.source_status !='ACTIVE';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_4)
    listing_conn.commit()
    
    
    query_5 = """
    update public.listing_change l set source_status='ACTIVE',batch_id = {1}
    from stage.temp_update_idx_status t
    join listing li
    on li.id=t.listing_id
    join listing_status ls
    on li.listing_status_id=ls.id
    where l.listing_id = t.listing_id
    and ls.display_flag is true
    and t.source_id = {0}
    and upper(ls.ylopo_status) != 'SOLD'
    and l.source_status !='ACTIVE';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_5)
    listing_conn.commit()
    
    
    query_6 = """
    update public.listing l set source_status='SOLD',batch_id = {1}, inactive_date = null
    from stage.temp_update_idx_status t
    where l.id = t.listing_id 
    and l.listing_status_id in (select id from listing_status where source_id = {0} and display_flag is true and upper(ylopo_status) = 'SOLD')
    and t.source_id = {0}
    and l.source_status != 'SOLD';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_6)
    listing_conn.commit()
    
    
    query_7 = """
    update public.listing_address l set source_status='SOLD',batch_id = {1}
    from stage.temp_update_idx_status t
    join listing li
    on li.id=t.listing_id
    join listing_status ls
    on li.listing_status_id=ls.id
    where l.listing_id = t.listing_id
    and ls.display_flag is true
    and upper(ls.ylopo_status) = 'SOLD'
    and t.source_id = {0}
    and l.source_status !='SOLD';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_7)
    listing_conn.commit()
    
    
    query_8 = """
    update public.listing_address_standard las set source_status='SOLD',batch_id = {1}
    where las.listing_address_id in (select la.id from listing_address la 
    join stage.temp_update_idx_status t
    on t.listing_id=la.listing_id
    join listing l 
    on l.id=la.listing_id  
    join listing_status ls
    on l.listing_status_id=ls.id
    where ls.display_flag is true
    and t.source_id = {0}
    and upper(ls.ylopo_status) = 'SOLD')
    and las.source_status !='SOLD';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_8)
    listing_conn.commit()
    
    
    query_9 = """
    update public.listing_change l set source_status='SOLD',batch_id = {1}
    from stage.temp_update_idx_status t
    join listing li
    on li.id=t.listing_id
    join listing_status ls
    on li.listing_status_id=ls.id
    where l.listing_id = t.listing_id
    and ls.display_flag is true
    and t.source_id = {0}
    and upper(ls.ylopo_status) = 'SOLD'
    and l.source_status != 'SOLD';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_9)
    listing_conn.commit()
    
    query_10 = """
    update public.listing l set source_status='SOLD',batch_id = {1}
    from stage.temp_update_idx_status t
    where l.id = t.listing_id 
    and l.listing_status_id in (select id from listing_status where source_id = {0} and display_flag is false and load_flag is true and upper(ylopo_status) = 'SOLD')
    and t.source_id = {0}
    and l.source_status != 'SOLD';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_10)
    listing_conn.commit()
    
    query_11 = """
    update public.listing_address l set source_status='SOLD',batch_id = {1}
    from stage.temp_update_idx_status t
    join listing li
    on li.id=t.listing_id
    join listing_status ls
    on li.listing_status_id=ls.id
    where l.listing_id = t.listing_id
    and ls.display_flag is false
    and ls.load_flag is true 
    and upper(ls.ylopo_status) = 'SOLD'
    and t.source_id = {0}
    and l.source_status !='SOLD';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_11)
    listing_conn.commit()
    
    
    
    query_12 = """
    update public.listing_address_standard las set source_status='SOLD',batch_id = {1}
    where las.listing_address_id in (select la.id from listing_address la 
    join stage.temp_update_idx_status t
    on t.listing_id=la.listing_id
    join listing l 
    on l.id=la.listing_id  
    join listing_status ls
    on l.listing_status_id=ls.id
    where ls.display_flag is false
    and ls.load_flag is true
    and t.source_id = {0}
    and upper(ls.ylopo_status) = 'SOLD')
    and las.source_status !='SOLD';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_12)
    listing_conn.commit()
    
    
    query_13 = """
    update public.listing_change l set source_status='SOLD',batch_id = {1}
    from stage.temp_update_idx_status t
    join listing li
    on li.id=t.listing_id
    join listing_status ls
    on li.listing_status_id=ls.id
    where l.listing_id = t.listing_id
    and ls.display_flag is false
    and ls.load_flag is true
    and t.source_id = {0}
    and upper(ls.ylopo_status) = 'SOLD'
    and l.source_status != 'SOLD';
    """.format(source_id,batch_id)
    listing_cursor.execute(query_13)
    listing_conn.commit()
    
    query_14 = """
    update listing l set source_status='INACTIVE',inactive_date=current_timestamp
    from 
    (
    select l.id as listing_id,l.source_status from listing l 
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    )a
    where l.id=a.listing_id
    and l.source_id = {0}
    and l.batch_id = {1};
    """.format(source_id,batch_id)
    listing_cursor.execute(query_14)
    listing_conn.commit()
    
    
    
    query_15 = """
    update listing l set source_status='INACTIVE' ,inactive_date=current_timestamp
    from 
    (
    select l.id as listing_id,l.source_status from listing l 
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    )a
    where l.id=a.listing_id
    and l.source_id = {0}
    and l.batch_id = {1};
    """.format(source_id,batch_id)
    listing_cursor.execute(query_15)
    listing_conn.commit()
    
    
    query_16 = """
    update listing l set source_status='INACTIVE' ,inactive_date=current_timestamp
    from 
    (
    select l.id as listing_id,l.source_status from listing l 
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    )a
    where l.id=a.listing_id
    and l.source_id = {0}
    and l.batch_id = {1};
    """.format(source_id,batch_id)
    listing_cursor.execute(query_16)
    listing_conn.commit()
    
    
    query_17 = """
    update listing_address l set source_status='INACTIVE' 
    from
    (
    select l.id as listing_id,la.source_status from listing_address la
    join listing l
    on l.id= la.listing_id
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    
    )a
    where l.source_id = {0}
    and l.batch_id = {1}
    and l.listing_id=a.listing_id;
    """.format(source_id,batch_id)
    listing_cursor.execute(query_17)
    listing_conn.commit()
    
    
    query_18 = """
    update listing_address l set source_status='INACTIVE' 
    from
    (
    select l.id as listing_id,la.source_status from listing_address la
    join listing l
    on l.id= la.listing_id
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    
    )a
    where l.source_id = {0}
    and l.batch_id = {1}
    and l.listing_id=a.listing_id;
    """.format(source_id,batch_id)
    listing_cursor.execute(query_18)
    listing_conn.commit()
    
    
    query_19 = """
    update listing_address l set source_status='INACTIVE' 
    from
    (
    select l.id as listing_id,la.source_status from listing_address la
    join listing l
    on l.id= la.listing_id
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    
    )a
    where l.source_id = {0}
    and l.batch_id = {1}
    and l.listing_id=a.listing_id;
    """.format(source_id,batch_id)
    listing_cursor.execute(query_19)
    listing_conn.commit()
    
    
    query_20 = """
    update listing_address_standard l set source_status='INACTIVE' 
    from
    (
    select l.id as listing_id,la.source_status from listing_address_standard la
    join listing l
    on l.id= la.listing_id
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    
    )a
    where l.source_id = {0}
    and l.batch_id = {1}
    and l.listing_id=a.listing_id;
    """.format(source_id,batch_id)
    listing_cursor.execute(query_20)
    listing_conn.commit()
    
    
    
    
    
    query_21 = """
    update listing_address_standard l set source_status='INACTIVE' 
    from
    (
    select l.id as listing_id,la.source_status from listing_address_standard la
    join listing l
    on l.id= la.listing_id
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    
    )a
    where l.source_id = {0}
    and l.batch_id = {1}
    and l.listing_id=a.listing_id;
    """.format(source_id,batch_id)
    listing_cursor.execute(query_21)
    listing_conn.commit()
    
    
    query_22 = """
    update listing_address_standard l set source_status='INACTIVE' 
    from
    (
    select l.id as listing_id,la.source_status from listing_address_standard la
    join listing l
    on l.id= la.listing_id
    join stage.temp_update_idx_status e
    on l.id=e.listing_id
    join listing_status s 
    on l.listing_status_id=s.id
    where l.source_id = {0}
    and s.marketing_status='EXPIRED'
    
    )a
    where l.source_id = {0}
    and l.batch_id = {1}
    and l.listing_id=a.listing_id;
    """.format(source_id,batch_id)
    listing_cursor.execute(query_22)
    listing_conn.commit()
    
    
    query_23 = """
    update listing_p_active l set expired_date=NULL 
    from 
    (
    select l.expired_date,ls.status,l.id as listing_id
    from listing_p_active l 
    join listing_status ls 
    on l.listing_status_id=ls.id
    where l.source_id = {0} and l.expired_date is not null
    and ls.marketing_status!='EXPIRED'
    )a
    
    where l.source_id = {0} and l.expired_date is not null
    and l.id=a.listing_id;
    """.format(source_id,batch_id)
    listing_cursor.execute(query_23)
    listing_conn.commit()
    
    if source_id == 642:
        retain_primary_images= f"""
                            WITH PrimaryPhoto AS (
                            
                                                    select MIN(lp.ID) as ID, lp.listing_id , s.source_id 
                                                    from listing_p_sold s
                                                    join listing_photo lp                on lp.listing_id = s.id and s.source_id = 642  
                                                    join stage.temp_update_idx_status sl on sl.listing_id = s.id and sl.source_id = 642 
                                                                                         and sl.source_id = s.source_id 
                                                    group by 2, 3 
                            )

                            DELETE from listing_photo  lp using PrimaryPhoto pp where   pp.listing_id = lp.listing_id  and pp.id <> lp.id;
                            """
        listing_cursor.execute(retain_primary_images)
        listing_conn.commit()
    
def append_to_existing_parquet(
    new_df, source_id, source_type, source_name, batch_id, class_Name, ):
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("bucket_name")
    
    # Construct the S3 path (same as etl_action lambda uses)
    filename = f"{source_name}_{class_Name}.parquet"
    folder_path = f"{source_type}/{source_id}_{source_name}/{batch_id}/{class_Name}/"
    s3_key = folder_path + filename
    
    log_msg = {
        "Status": "INACTIVE LAMBDA - Starting S3 append operation",
        "s3_key": s3_key,
        "bucket": bucket_name,
        "new_inactive_rows": len(new_df),
    }
    # print(json.dumps(log_msg))          
   
    new_df.columns = new_df.columns.astype(str).str.replace(".", "_")
    new_df = new_df.astype(str).replace(["nan", "None", ""], None)
    
    existing_df = None
    
    # Download existing parquet file
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        existing_buffer = io.BytesIO(response['Body'].read())
        existing_df = pd.read_parquet(existing_buffer, engine="pyarrow")
        
        log_msg = {
            "Status": "Found existing parquet file in S3",
            "s3_key": s3_key,
            "existing_rows": len(existing_df),
            "existing_action_types": existing_df["action_type"].value_counts().to_dict() if "action_type" in existing_df.columns else {},
            "new_inactive_rows": len(new_df),
        }
        print(json.dumps(log_msg))
        
    except ClientError as e:
       
        error_code = e.response['Error']['Code']
        
        if error_code == 'NoSuchKey':
            log_msg = {
                "Status": "NO EXISTING FILE - Will create new parquet with INACTIVE rows only",
                "s3_key": s3_key,
                "note": "This is unexpected - active batch should have created this file",
            }
            print(json.dumps(log_msg))
        else:
            log_msg = {
                "Status": "ERROR downloading existing parquet",
                "s3_key": s3_key,
                "error_code": error_code,
                "error_message": str(e),
            }
            print(json.dumps(log_msg))
            raise  # Re-raise if it's not NoSuchKey              
    except Exception as e:
        log_msg = {
            "Status": "Error downloading existing parquet",
            "s3_key": s3_key,
            "Error": str(e),
            "error_type": type(e).__name__,
        }
        print(json.dumps(log_msg))
        raise     
    
    #Step 2: Combine existing + new data ─────────────────────────────────
    if existing_df is not None:
        # Ensure both DataFrames have same columns
        all_columns = list(set(existing_df.columns) | set(new_df.columns))
        
        # Add missing columns with None values
        for col in all_columns:
            if col not in existing_df.columns:
                existing_df[col] = None
            if col not in new_df.columns:
                new_df[col] = None
        
        # Reorder columns to match
        existing_df = existing_df[all_columns]
        new_df = new_df[all_columns]
        
        # Concatenate
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
    else:
        combined_df = new_df
    # ── Step 3: Upload combined parquet back to S3 ──────────────────────────
    buffer = io.BytesIO()
    combined_df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    
    ensure_folder_structure(s3, bucket_name, folder_path)
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
    
    log_msg = {
        "Status": "Uploaded combined parquet to S3",
        "s3_key": s3_key,
        "bucket": bucket_name,
        "final_row_count": len(combined_df),
        "final_action_breakdown": combined_df["action_type"].value_counts().to_dict() if "action_type" in combined_df.columns else {},
        }
    # print(json.dumps(log_msg))

def etl_get_missing_listings (source_id, homelisting_cursor, homelisting_conn, rds_cursor, rds_connection):
    # Thsi function is to get missing listings from direct_idx_id table which are not present in listing_p_active table
    Missing_Listings_query = f"""
        
        SELECT DISTINCT '1990-01-01' as modification_timestamp , source_listing_id as listingkey  , 
                        true as download_flag , inv.source_id , false as respecs_flag ,
                        '1990-01-01' as media_modification_timestamp
                        --,inv.status
        FROM stage.direct_idx_id inv
		join listing_status 	  ls  on lower(ls.status) = lower(inv.status) 
									  and ls.ylopo_status in ('ACTIVE', 'PENDING')
									  and inv.source_id = {source_id}
									  and ls.source_id = {source_id}
        WHERE inv.source_id = {source_id}
        AND NOT EXISTS  (
              SELECT 1
              FROM listing_p_active l
              WHERE l.source_listing_id = inv.source_listing_id
			  and l.source_id = {source_id}
          );
		  
    """
    homelisting_cursor.execute(Missing_Listings_query)
    
    batch_size = 1000
    
    insert_query ="""
        INSERT INTO idx_stage.temp_table
            (modification_timestamp, listingkey, download_flag,source_id,respecs_flag,media_modification_timestamp)
        VALUES %s
    """
 
    try:
        while True:

            rows = homelisting_cursor.fetchmany(batch_size)

            if not rows:
                break

            execute_values(
                rds_cursor,
                insert_query,
                rows,
                page_size=batch_size
            )

            rds_connection.commit()
       
        return (homelisting_cursor.rowcount)
 
    except Exception as e:
        
        raise
        
def lambda_handler(event, context):

    logger.info(event)
    # run_host = event['run_host']
    source_id = event['source_id']
    source_type = event['source_type']
    mls_board = event['mls_board']
    batch_id = event['batch_id']
    inactive_threshold = event['inactive_threshold']
    final_response = event
    
    try:
        
        secret_name = os.environ.get('rdsDatabase')
        listing_secret = os.environ.get('listingDatabase')
        secrets = fetch_secrets(secret_name)
        listing_secrets = fetch_secrets(listing_secret)
        connection = setup_db_connection(secrets)
        listing_conn = setup_db_connection(listing_secrets)
        cursor = connection.cursor()
        listing_cursor = listing_conn.cursor()
        
        etl_direct_idx_missing_delete_listings(source_type,source_id,batch_id,listing_cursor,listing_conn,cursor,connection)
        threshold_value = threshold_calculation(source_id,batch_id,listing_cursor,listing_conn)
        if source_type == 'SourceRE API':
            etl_get_missing_listings( source_id,listing_cursor, listing_conn,cursor,connection)
        if threshold_value is None:
            final_response['threshold_calculation'] = 'no inactive listings'
            final_response['success'] = True 
            final_status = """update stage.etl_batches set load_missing_lst_status = 'No Inactive Listings' where batch_id = {}""".format(batch_id)
            listing_cursor.execute(final_status)
            listing_conn.commit()
            inactive_status_marking(source_id,batch_id,listing_cursor,listing_conn)
            etl_direct_idx_missing_status_delete_listings(source_id,batch_id,listing_cursor,listing_conn)
            listing_status_update(source_id,batch_id,listing_cursor,listing_conn)
            final_response['threshold_status'] = 'Threshold Not Exceeded'
            return final_response
        else:
            threshlod_update = """update stage.etl_batches set inactive_threshold = {0} where batch_id = {1}""".format(threshold_value,batch_id)
            listing_cursor.execute(threshlod_update)
            listing_conn.commit()
            
            if threshold_value < inactive_threshold:
                listing_change_insert(source_id,batch_id,listing_cursor,listing_conn)
                in_active_marking(source_id,batch_id,listing_cursor,listing_conn)
                final_status = """update stage.etl_batches set load_missing_lst_status = 'Completed' where batch_id = {}""".format(batch_id)
                listing_cursor.execute(final_status)
                listing_conn.commit()
                final_response['threshold_calculation'] = True
                final_response['success'] = True
                inactive_status_marking(source_id,batch_id,listing_cursor,listing_conn)
                etl_direct_idx_missing_status_delete_listings(source_id,batch_id,listing_cursor,listing_conn)
                listing_status_update(source_id,batch_id,listing_cursor,listing_conn)
                final_response['threshold_status'] = 'Threshold Not Exceeded'
                return final_response
    
            else:
                final_response['threshold_calculation'] = True
                final_response['threshold_status'] = 'Threshold Exceeded'
                final_status = """update stage.etl_batches set load_missing_lst_status = 'Threshold Exceeded' where batch_id = {}""".format(batch_id)
                listing_cursor.execute(final_status)
                listing_conn.commit()
                final_response['success'] = True
                inactive_status_marking(source_id,batch_id,listing_cursor,listing_conn)
                etl_direct_idx_missing_status_delete_listings(source_id,batch_id,listing_cursor,listing_conn)
                listing_status_update(source_id,batch_id,listing_cursor,listing_conn)
                return final_response
            

    except Exception as e:
        final_response['threshold_calculation'] = False
        
        log_msg = {  'Error':e , "Error At line": traceback.format_exc(),"Payload" :final_response}
        logger.error(log_msg)

        return final_response

    finally:

        if listing_cursor:
            listing_cursor.close()
        if listing_conn:
            listing_conn.close()
