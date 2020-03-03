import boto3
import psycopg2
import configparser

create_impressions_staging_statement: str = (
    """
    CREATE TABLE IF NOT EXISTS public.impressions_staging(
        Meta_schema varchar(20),
        Meta_version varchar(20),
        GdprComputed boolean,
        GdprSource varchar(100),
        RemoteIP varchar(20),
        UserAgent varchar(100),
        Ecpm integer,
        Datacenter boolean,
        BurnIn boolean,
        IsValidUA boolean,
        User_info varchar(100),
        UserKey varchar(20),
        ImpressionCount integer,
        Id varchar(100),
        DecisionId varchar(100),
        DecisionIdx integer,
        CreatedOn varchar(100),
        EventCreatedOn bigint,
        ImpressionCreatedOn bigint,
        AdTypeId integer,
        AuctionBids integer,
        BrandId integer,
        CampaignId integer,
        Categories varchar(100),
        ChannelId integer,
        CreativeId integer,
        CreativePassId integer,
        DeliveryMode integer,
        Device varchar(256),
        FirstChannelId integer,
        IsNoTrack boolean,
        IsTrackingCookieEvents boolean,
        IsPublisherPayoutExempt boolean,
        Keywords varchar(256),
        MatchingKeywords varchar(256),
        NetworkId integer,
        PassId integer,
        PhantomCreativePassId integer,
        PlacementName varchar(100),
        PhantomPassId integer,
        PriorityId integer,
        Price decimal(8,2),
        RateType integer,
        RelevancyScore integer,
        Revenue decimal(8,2),
        NetRevenue decimal(8,2),
        GrossRevenue decimal(8,2),
        ServedBy varchar(100),
        ServedByPid integer,
        ServedByAsg varchar(100),
        SiteId integer,
        Url varchar(1000),
        ZoneId integer);
    """
)


create_clicks_staging_statement: str = (
    """
    CREATE TABLE IF NOT EXISTS public.clicks_staging(
        Meta_schema varchar(20),
        Meta_version varchar(20),
        GdprComputed boolean,
        GdprSource varchar(100),
        RemoteIP varchar(20),
        UserAgent varchar(100),
        Ecpm integer,
        Datacenter boolean,
        BurnIn boolean,
        IsValidUA boolean,
        User_info varchar(100),
        UserKey varchar(20),
        ClickCount integer,
        Id varchar(100),
        CreatedOn varchar(100),
        EventCreatedOn bigint,
        ImpressionCreatedOn bigint,
        AdTypeId integer,
        BrandId integer,
        CampaignId integer,
        Categories varchar(100),
        ChannelId integer,
        CreativeId integer,
        CreativePassId integer,
        DeliveryMode integer,
        FirstChannelId integer,
        ImpressionId varchar(100),
        DecisionId varchar(100),
        IsNoTrack boolean,
        IsTrackingCookieEvents boolean,
        Keywords varchar(256),
        Device varchar(256),
        MatchingKeywords varchar(256),
        NetworkId integer,
        PassId integer,
        PhantomCreativePassId integer,
        PlacementName varchar(100),
        PhantomPassId integer,
        Price decimal(8,2),
        PriorityId integer,
        RateType integer,
        Revenue decimal(8,2),
        ServedBy varchar(100),
        ServedByPid integer,
        ServedByAsg varchar(100),
        SiteId integer,
        Url varchar(1000),
        ZoneId integer);
    """
)

create_impressions_view_statement: str = (
    """                                 
CREATE OR REPLACE VIEW public.impressions as 
    (SELECT Meta_schema, Meta_version, GdprComputed, GdprSource, RemoteIP, UserAgent, Ecpm, Datacenter, BurnIn, IsValidUA, UserKey, 
        decode(json_extract_path_text(User_info, 'IsNew'), 'false', false, 'true', true, false) as UserIsNew, 
        ImpressionCount, Id, DecisionId, DecisionIdx, CreatedOn, EventCreatedOn, ImpressionCreatedOn, AdTypeId, AuctionBids, BrandId, 
        CampaignId, Categories, ChannelId, CreativeId, CreativePassId, DeliveryMode, 
        json_extract_path_text(device, 'brandName')::varchar(20) as Device_brandName, 
        json_extract_path_text(device, 'modelName')::varchar(20) as Device_modelName, 
        json_extract_path_text(device, 'osRawVersion')::varchar(20) as Device_osRawVersion, 
        json_extract_path_text(device, 'osMajorVersion')::integer as Device_osMajorVersion, 
        json_extract_path_text(device, 'osMinorVersion')::integer as Device_osMinorVersion, 
        json_extract_path_text(device, 'browser')::varchar(20) as Device_browser, 
        json_extract_path_text(device, 'browserRawVersion')::varchar(20) as Device_browserRawVersion, 
        json_extract_path_text(device, 'browserMajorVersion')::integer as Device_browserMajorVersion, 
        json_extract_path_text(device, 'browserMinorVersion')::integer as Device_browserMinorVersion, 
        json_extract_path_text(device, 'formFactor')::varchar(20) as Device_formFactor, 
        FirstChannelId, IsNoTrack, IsTrackingCookieEvents, IsPublisherPayoutExempt, Keywords, MatchingKeywords, NetworkId, PassId, 
        PhantomCreativePassId, PlacementName, PhantomPassId, PriorityId, Price, RateType, RelevancyScore, Revenue, NetRevenue, 
        GrossRevenue, ServedBy, ServedByPid, ServedByAsg, SiteId, Url, ZoneId 
    FROM public.impressions_staging);
    """
)

create_clicks_view_statement: str = (
    """
CREATE OR REPLACE VIEW public.clicks as 
    (SELECT Meta_schema, Meta_version, GdprComputed, GdprSource, RemoteIP, UserAgent, Ecpm, Datacenter, BurnIn, IsValidUA, UserKey, 
        decode(json_extract_path_text(User_info, 'IsNew'), 'false', false, 'true', true, false) as UserIsNew, 
        ClickCount, Id, CreatedOn, EventCreatedOn, ImpressionCreatedOn, AdTypeId, BrandId, CampaignId, Categories, ChannelId, CreativeId, 
        CreativePassId, DeliveryMode, FirstChannelId, ImpressionId, DecisionId, IsNoTrack, IsTrackingCookieEvents, Keywords, 
        json_extract_path_text(device, 'brandName')::varchar(20) as Device_brandName, 
        json_extract_path_text(device, 'modelName')::varchar(20) as Device_modelName, 
        json_extract_path_text(device, 'osRawVersion')::varchar(20) as Device_osRawVersion, 
        json_extract_path_text(device, 'osMajorVersion')::integer as Device_osMajorVersion, 
        json_extract_path_text(device, 'osMinorVersion')::integer as Device_osMinorVersion, 
        json_extract_path_text(device, 'browser')::varchar(20) as Device_browser, 
        json_extract_path_text(device, 'browserRawVersion')::varchar(20) as Device_browserRawVersion, 
        json_extract_path_text(device, 'browserMajorVersion')::integer as Device_browserMajorVersion, 
        json_extract_path_text(device, 'browserMinorVersion')::integer as Device_browserMinorVersion, 
        json_extract_path_text(device, 'formFactor')::varchar(20) as Device_formFactor, 
        MatchingKeywords, NetworkId, PassId, PhantomCreativePassId, PlacementName, PhantomPassId, Price, PriorityId, RateType, 
        Revenue, ServedBy, ServedByPid, ServedByAsg, SiteId, Url, ZoneId 
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




