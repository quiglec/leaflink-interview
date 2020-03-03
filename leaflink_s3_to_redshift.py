import boto3
import psycopg2
import configparser

create_impressions_staging_statement: str = (
    """
    CREATE TABLE IF NOT EXISTS public.impressions_staging(
        meta_schema varchar(20),
        meta_version varchar(20),
        gdpr_computed boolean,
        gdpr_source varchar(100),
        remote_ip varchar(20),
        user_agent varchar(100),
        ecpm integer,
        datacenter boolean,
        burn_in boolean,
        is_valid_ua boolean,
        user_info varchar(100),
        user_key varchar(20),
        impression_count integer,
        id varchar(100),
        decision_id varchar(100),
        decision_idx integer,
        created_on varchar(100),
        event_created_on bigint,
        impression_created_on bigint,
        ad_type_id integer,
        auction_bids integer,
        brand_id integer,
        campaign_id integer,
        categories varchar(100),
        channel_id integer,
        creative_id integer,
        creative_pass_id integer,
        delivery_mode integer,
        device varchar(256),
        first_channel_id integer,
        is_no_track boolean,
        is_tracking_cookie_events boolean,
        is_publisher_payout_exempt boolean,
        keywords varchar(256),
        matching_keywords varchar(256),
        network_id integer,
        pass_id integer,
        phantom_creative_pass_id integer,
        placement_name varchar(100),
        phantom_pass_id integer,
        priority_id integer,
        price decimal(8,2),
        rate_type integer,
        relevancy_score integer,
        revenue decimal(8,2),
        net_revenue decimal(8,2),
        gross_revenue decimal(8,2),
        served_by varchar(100),
        served_by_pid integer,
        served_by_asg varchar(100),
        site_id integer,
        url varchar(1000),
        zone_id integer);
    """
)

create_clicks_staging_statement: str = (
    """
    CREATE TABLE IF NOT EXISTS public.clicks_staging(
        meta_schema varchar(20),
        meta_version varchar(20),
        gdpr_computed boolean,
        gdpr_source varchar(100),
        remote_ip varchar(20),
        user_agent varchar(100),
        ecpm integer,
        datacenter boolean,
        burn_in boolean,
        is_valid_ua boolean,
        user_info varchar(100),
        user_key varchar(20),
        click_count integer,
        id varchar(100),
        created_on varchar(100),
        event_created_on bigint,
        impression_created_on bigint,
        ad_type_id integer,
        brand_id integer,
        campaign_id integer,
        categories varchar(100),
        channel_id integer,
        creative_id integer,
        creative_pass_id integer,
        delivery_mode integer,
        first_channel_id integer,
        impression_id varchar(100),
        decision_id varchar(100),
        is_no_track boolean,
        is_tracking_cookie_events boolean,
        keywords varchar(256),
        device varchar(256),
        matching_keywords varchar(256),
        network_id integer,
        pass_id integer,
        phantom_creative_pass_id integer,
        placement_name varchar(100),
        phantom_pass_id integer,
        price decimal(8,2),
        priority_id integer,
        rate_type integer,
        revenue decimal(8,2),
        served_by varchar(100),
        served_by_pid integer,
        served_by_asg varchar(100),
        site_id integer,
        url varchar(1000),
        zone_id integer);
    """
)

create_impressions_view_statement: str = (
    """                                 
CREATE OR REPLACE VIEW public.impressions as 
    (SELECT meta_schema, meta_version, gdpr_computed, gdpr_source, remote_ip, user_agent, ecpm, datacenter, burn_in, is_valid_ua, user_key, 
            decode(json_extract_path_text(user_info, 'IsNew'), 'false', false, 'true', true, false) as user_is_new, 
            impression_count, id, decision_id, decision_idx, created_on, event_created_on, impression_created_on, ad_type_id, auction_bids, brand_id, 
            campaign_id, categories, channel_id, creative_id, creative_pass_id, delivery_mode, 
            json_extract_path_text(device, 'brandName')::varchar(20) as device_brand_name, 
            json_extract_path_text(device, 'modelName')::varchar(20) as device_model_name, 
            json_extract_path_text(device, 'osRawVersion')::varchar(20) as device_os_raw_version, 
            json_extract_path_text(device, 'osMajorVersion')::integer as device_os_major_version, 
            json_extract_path_text(device, 'osMinorVersion')::integer as device_os_minor_version, 
            json_extract_path_text(device, 'browser')::varchar(20) as device_browser, 
            json_extract_path_text(device, 'browserRawVersion')::varchar(20) as device_browser_raw_version, 
            json_extract_path_text(device, 'browserMajorVersion')::integer as device_browser_major_version, 
            json_extract_path_text(device, 'browserMinorVersion')::integer as device_browser_minor_version, 
            json_extract_path_text(device, 'formFactor')::varchar(20) as device_form_factor, 
            first_channel_id, is_no_track, is_tracking_cookie_events, is_publisher_payout_exempt, keywords, matching_keywords, network_id, pass_id, 
            phantom_creative_pass_id, placement_name, phantom_pass_id, priority_id, price, rate_type, relevancy_score, revenue, net_revenue, 
            gross_revenue, served_by, served_by_pid, served_by_asg, site_id, url, zone_id 
    FROM public.impressions_staging);
    """
)

create_clicks_view_statement: str = (
    """
CREATE OR REPLACE VIEW public.clicks as 
    (SELECT meta_schema, meta_version, gdpr_computed, gdpr_source, remote_ip, user_agent, ecpm, datacenter, burn_in, is_valid_ua, user_key, 
            decode(json_extract_path_text(user_info, 'IsNew'), 'false', false, 'true', true, false) as user_is_new, 
            click_count, Id, created_on, event_created_on, impression_created_on, ad_type_id, brand_id, campaign_id, categories, channel_id, creative_id, 
            creative_pass_id, delivery_mode, first_channel_id, impression_id, decision_id, is_no_track, is_tracking_cookie_events, keywords, 
            json_extract_path_text(device, 'brandName')::varchar(20) as device_brand_name, 
            json_extract_path_text(device, 'modelName')::varchar(20) as device_model_name, 
            json_extract_path_text(device, 'osRawVersion')::varchar(20) as device_os_raw_version, 
            json_extract_path_text(device, 'osMajorVersion')::integer as device_os_major_version, 
            json_extract_path_text(device, 'osMinorVersion')::integer as device_os_minor_version, 
            json_extract_path_text(device, 'browser')::varchar(20) as device_browser, 
            json_extract_path_text(device, 'browserRawVersion')::varchar(20) as device_browser_raw_version, 
            json_extract_path_text(device, 'browserMajorVersion')::integer as device_browser_major_version, 
            json_extract_path_text(device, 'browserMinorVersion')::integer as device_browser_minor_version, 
            json_extract_path_text(device, 'formFactor')::varchar(20) as device_form_factor, 
            matching_keywords, network_id, pass_id, phantom_creative_pass_id, placement_name, phantom_pass_id, price, priority_id, rate_type, 
            revenue, served_by, served_by_pid, served_by_asg, site_id, url, zone_id 
    FROM public.clicks_staging);
    """
)

def create_impressions_staging(conn: psycopg2.extensions.connection, create_impressions_staging_statement: str):
    """Creates impressions_staging table"""
    cur = conn.cursor()
    cur.execute(create_impressions_staging_statement)
    conn.commit()
    print("Successfully created impressions_staging table")

def create_clicks_staging(conn: psycopg2.extensions.connection, create_clicks_staging_statement: str):
    """Creates clicks_staging table"""
    cur = conn.cursor()
    cur.execute(create_clicks_staging_statement)
    conn.commit()
    print("Successfully created clicks_staging table")

def create_impressions_view(conn: psycopg2.extensions.connection, create_impressions_view_statement: str):
    """Creates impressions view"""
    cur = conn.cursor()
    cur.execute(create_impressions_view_statement)
    conn.commit()
    print("Successfully created impressions view")

def create_clicks_view(conn: psycopg2.extensions.connection, create_clicks_view_statement: str):
    """Creates clicks view"""
    cur = conn.cursor()
    cur.execute(create_clicks_view_statement)
    conn.commit()
    print("Successfully created clicks view")

def load_impressions_staging_statement(bucket: str, prefix: str, iam_role: str, json_path: str):
    """Creates command for loading impressions data from s3 to impressions_staging"""
    load_impressions_staging_statement: str = (
        f"""copy impressions_staging 
        from 's3://{bucket}/{prefix}/impressions' 
        iam_role '{iam_role}' 
        json 's3://{json_path}/impression_jsonpath.json';
        """
    )
    return load_impressions_staging_statement

def load_impressions_staging(conn: psycopg2.extensions.connection, load_impressions_staging_statement :str):
    """Loads impressions data from s3 to impressions_staging"""
    cur = conn.cursor()
    cur.execute(load_impressions_staging_statement)

def load_clicks_staging_statement(bucket: str, prefix: str, iam_role: str, json_path: str):
    """Creates command for loading clicks data from s3 to clicks_staging"""
    load_clicks_staging_statement: str = (
        f"""copy clicks_staging 
        from 's3://{bucket}/{prefix}/clicks' 
        iam_role '{iam_role}' 
        json 's3://{json_path}/click_jsonpath.json';
        """
    )
    return load_clicks_staging_statement

def load_clicks_staging(conn: psycopg2.extensions.connection, load_clicks_staging_statement :str):
    """Loads clicks data from s3 to clicks_staging"""
    cur = conn.cursor()
    cur.execute(load_clicks_staging_statement)

def get_file_prefixes(bucket: str):
    """Get set of prefixes for all files in the bucket"""
    s3 = boto3.client('s3')
    prefixes = set()
    kwargs = {'Bucket': bucket}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            prefixes.add("/".join(obj['Key'].split("/")[:-1]))
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return prefixes

def main():
    try:
        print("Retrieving Redshift and S3 info...")
        config = configparser.ConfigParser()
        config.read('config.ini')
        dbname = config['Redshift']['dbname']
        host = config['Redshift']['host']
        port = config['Redshift']['port']
        user = config['Redshift']['user']
        password = config['Redshift']['password']
        iam_role = config['Redshift']['iam_role']
        impressions_jsonpath = config['S3']['impressions_jsonpath']
        clicks_jsonpath = config['S3']['clicks_jsonpath']
        bucket = config['S3']['data_bucket']
        print("Redshift info retrieved")
    except Exception as e:
        print(str(e))
        print("Unable to retrieve Redshift info from config.ini. Exiting.")
        return -1

    try:
        print("Connecting to Redshift...")
        conn = psycopg2.connect(
            dbname = dbname, 
            host = host, 
            port = port, 
            user = user, 
            password = password
        )
        cur = conn.cursor()
        print("Connection successful")
    except Exception as e:
        print(str(e))
        print("Unable to connect to Redshift. Exiting.")
        conn.close()
        return -1

    try:
        print("Creating staging tables...")
        create_impressions_staging(conn, create_impressions_staging_statement)
        create_clicks_staging(conn, create_clicks_staging_statement)
        conn.commit()
    except Exception as e:
        print(str(e))
        print("Unable to create staging tables. Exiting.")
        conn.rollback()
        conn.close()
        return -1

    try:
        print("Creating views...")
        create_impressions_view(conn, create_impressions_view_statement)
        create_clicks_view(conn, create_clicks_view_statement)
        conn.commit()
    except Exception as e:
        print(str(e))
        print("Unable to create views. Exiting.")
        conn.rollback()
        conn.close()
        return -1

    try:
        print("Loading clicks_staging...")
        prefixes = get_file_prefixes(bucket)
        for prefix in prefixes:
            print(load_clicks_staging_statement(bucket, prefix, iam_role, clicks_jsonpath))
            load_clicks_staging(conn, load_clicks_staging_statement(bucket, prefix, iam_role, clicks_jsonpath))
        print("Loading impressions_staging...")
        for prefix in prefixes:
            print(load_impressions_staging_statement(bucket, prefix, iam_role, impressions_jsonpath))
            load_impressions_staging(conn, load_impressions_staging_statement(bucket, prefix, iam_role, impressions_jsonpath))
        conn.commit()
        print("Data loaded successfully")
    except Exception as e:
        print(str(e))
        print("Unable to load staging tables. Exiting.")
        conn.rollback()
        conn.close()
        return -1

    print("Tables and views created. Data loaded. Exiting.")
    conn.close()
    return 0

if __name__ == "__main__":
    main()
