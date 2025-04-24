# change here - All imports are defined here
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_timestamp,month,count,\
                                    size,count_distinct,when,hour,weekday, \
                                    split,element_at,\
                                    avg,round
import os, re, argparse
import logging
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------
#### Logging Pending

# ----------------------------------------------------------------------------------------
# change here - Initating spark session
# Default using all cores
# 100% CPU utilization while running Stay Calm

def intiate_spark_connection():
    spark = SparkSession.builder\
        .appName('Verloop Monthly Newsletter Analytics')\
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.2") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.files.openCostInBytes", "4194304") \
        .config("spark.sql.broadcastTimeout", "300") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.5") \
        .getOrCreate()
    
    logger.info("Spark session initialized with optimized configuration")
    return spark

# ----------------------------------------------------------------------------------------
# Export incoming data lamda function - PENDING
# modified

def export_data(final_df, output_path=None,export_path=None,last_month=-1,clientid='default'):
    # Change here - Add client ID to separte outpur bucket if needed
    if output_path and export_path:
        logger.info(f"Writing data to {output_path}")
        final_df.write.mode("overwrite")\
            .option("compression", "snappy")\
            .parquet(f'{export_path}{clientid}/{last_month}/{output_path}')
    else:
        logger.info("Displaying sample data")
        final_df.show(10, truncate=False)
    # print(final_df.show())
    # Clear the dataframe from memory after export
    final_df.unpersist()
# ----------------------------------------------------------------------------------------
# Read all files from the path

def read_files(spark,file_list):
    logger.info(f"Reading files: {file_list}")
    main_df = spark.read\
        .option("header", True)\
        .option('multiline', True)\
        .option("inferSchema", True)\
        .option("timestampFormat", "dd-MM-yyyy HH:mm:ss")\
        .csv(file_list)
    
    # Cache the main dataframe as it's used across multiple operations
    main_df.cache()
    logger.info(f"Data loaded and cached. Total rows: {main_df.count()}")
    return main_df

# ----------------------------------------------------------------------------------------
# Change here - Chat_Metrics - Last 6 Months

def chat_metrics(main_df,clientid,export_path,last_month):
    logger.info("Processing chat metrics")
    
    # Create base dataframe with only required columns
    base_df = main_df.filter(
        (main_df.RoomStatus == 'RESOLVED')
    ).select(
        main_df.RoomCode,
        to_timestamp(main_df.ChatEndTime, format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
        main_df.UserId,
        main_df.ClosedBy
    ).cache()
    
    # Calculate all metrics in a single pass
    metrics = base_df.groupBy(
        month(col('ChatEndTime')).alias("month")
    ).agg(
        count(col('RoomCode')).alias("incoming_chats"),
        count_distinct(col('UserId')).alias('unique_users'),
        count(when(col('ClosedBy') == 'System', col('RoomCode'))).alias('closed_by_bot'),
        count(when(col('ClosedBy') != 'System', col('RoomCode'))).alias('closed_by_agent')
    )
    
    # Calculate recurring users
    recurring_users = base_df.groupBy(
        month(col('ChatEndTime')).alias('month'),
        col('UserId')
    ).agg(
        count(col('RoomCode')).alias('chat_count')
    ).filter(
        col('chat_count') > 1
    ).groupBy(
        col('month')
    ).agg(
        count(col('UserId')).alias('recurring_users')
    )
    
    # Join metrics and export
    final_metrics = metrics.join(
        recurring_users,
        metrics.month == recurring_users.month,
        "left"
    ).select(
        metrics.month,
        metrics.incoming_chats,
        metrics.unique_users,
        recurring_users.recurring_users,
        metrics.closed_by_bot,
        metrics.closed_by_agent
    )
    
    export_data(final_metrics, "output/chat_metrics",export_path,last_month,clientid)
    base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Change here - Channels/Channel Metric/Peak Hour Hourly Volume - Last 1 Month 

def channels(main_df,clientid,export_path,last_month):
    logger.info(f"Processing channel metrics for month {last_month}")
    
    base_df = main_df.select(
        main_df.RoomCode,
        to_timestamp(main_df.ChatEndTime, format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
        main_df.UserOs,
        main_df.ChatChannel,
    ).filter(
        (main_df.RoomStatus == 'RESOLVED') & 
        (month(col('ChatEndTime')) == last_month)
    ).cache()
    
    # Calculate all metrics in a single pass
    channel_metrics = base_df.groupBy(
        hour(col('ChatEndTime')).alias('hour')
    ).agg(
        count(col('RoomCode')).alias('total_chats'),
        count(when(col('ChatChannel') == 'LIVECHAT', col('RoomCode'))).alias('channel_livechat'),
        count(when(col('ChatChannel') == 'WHATSAPP', col('RoomCode'))).alias('channel_whatsapp'),
        count(when(~((col('ChatChannel') == 'LIVECHAT') | (col('ChatChannel') == 'WHATSAPP')), col('RoomCode'))).alias('channel_other'),
        count(when(col('UserOs').rlike('^Android*'), col('RoomCode'))).alias('device_android'),
        count(when(col('UserOs').rlike('^iOS*'), col('RoomCode'))).alias('device_ios'),
        count(when(col('UserOs').rlike('^macOS*'), col('RoomCode'))).alias('device_macos'),
        count(when(col('UserOs').rlike('^Windows*'), col('RoomCode'))).alias('device_windows'),
        count(when(~(
            col('UserOs').rlike('^Android*') |
            col('UserOs').rlike('^iOS*') |
            col('UserOs').rlike('^macOS*') |
            col('UserOs').rlike('^Windows*')
        ), col('RoomCode'))).alias('device_other')
    )
    
    export_data(channel_metrics, "output/channel_metrics",export_path,last_month,clientid)
    base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Optimized peak hour analysis
def peak_hour(main_df,clientid,export_path,last_month):
    logger.info(f"Processing peak hour metrics for month {last_month}")
    
    base_df = main_df.select(
        main_df.RoomCode,
        to_timestamp(main_df.ChatEndTime, format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
    ).filter(
        (main_df.RoomStatus == 'RESOLVED') & 
        (month(col('ChatEndTime')) == last_month)
    ).cache()
    
    # Calculate hourly and weekday metrics in a single pass
    hourly_metrics = base_df.groupBy(
        hour(col('ChatEndTime')).alias('hour')
    ).agg(
        count(col('RoomCode')).alias('total_chats')
    )
    
    weekday_metrics = base_df.groupBy(
        weekday(col('ChatEndTime')).alias('weekday')
    ).agg(
        count(col('RoomCode')).alias('total_chats')
    )
    
    export_data(hourly_metrics, "output/hourly_metrics",export_path,last_month,clientid)
    export_data(weekday_metrics, "output/weekday_metrics",export_path,last_month,clientid)
    base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Change here - CSAT Metrics - Last 6 Months
# modified
def csat_metrics(main_df,clientid,export_path,last_month):
    logger.info("Processing CSAT metrics")
    
    base_df = main_df.select(
        main_df.RoomCode,
        to_timestamp(main_df.CsatSubmittedAt, format='dd-MM-yyyy HH:mm:ss').alias('csat_submittedat'),
        to_timestamp(main_df.CsatTriggeredAt, format='dd-MM-yyyy HH:mm:ss').alias('csat_triggeredat'),
        main_df.UserId,
        main_df.ClosedBy,
        main_df.CsatScore
    ).filter(
        col('csat_triggeredat').isNotNull()
    ).cache()
    
    # Calculate all CSAT metrics in a single pass
    csat_metrics = base_df.groupBy(
        month(col('csat_triggeredat')).alias('month')
    ).agg(
        count(col('csat_triggeredat')).alias('total_triggered_csat'),
        count(when(col('csat_submittedat').isNotNull(), col('csat_submittedat'))).alias('total_submitted_csat'),
        count_distinct(when(col('csat_submittedat').isNotNull(), col('UserId'))).alias('unique_user_submitted_csat'),
        count(when((col('csat_submittedat').isNotNull() & (col('ClosedBy') == 'System')), col('csat_submittedat'))).alias('bot_submitted_csat'),
        count(when((col('csat_submittedat').isNotNull() & (col('ClosedBy') != 'System')), col('csat_submittedat'))).alias('agent_submitted_csat'),
        round(avg(when(col('csat_submittedat').isNotNull(), col('CsatScore'))), 2).alias('avg_csat_score')
    )
    
    # Calculate recurring user submissions
    recurring_users = base_df.filter(
        col('csat_submittedat').isNotNull()
    ).groupBy(
        month(col('csat_triggeredat')).alias('month_'),
        col('UserId')
    ).agg(
        count(col('csat_submittedat')).alias('submission_count')
    ).filter(
        col('submission_count') > 1
    ).groupBy(
        col('month_')
    ).agg(
        count(col('UserId')).alias('recurring_user_submitted_csat')
    )
    
    # Join and export final metrics
    final_metrics = csat_metrics.join(
        recurring_users,
        csat_metrics.month == recurring_users.month_,
        "left"
    )
    
    export_data(final_metrics, "output/csat_metrics",export_path,last_month,clientid)
    base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Change here - Menu_Metrics - Last 6 Months

def menu_metrics(main_df,clientid,export_path,last_month):
    logger.info("Processing menu metrics")
    
    # Find main menu column
    main_menu_columns = ['mainmenu', 'main_menu', 'main-menu']
    main_menu_col = None
    for col_name in main_df.columns:
        if col_name.lower() in main_menu_columns:
            main_menu_col = col_name
            break
    
    if main_menu_col:
        base_df = main_df.filter(
            (main_df.RoomStatus == 'RESOLVED') &
            (col(main_menu_col).isNotNull())
        ).select(
            main_df.RoomCode,
            to_timestamp(main_df.ChatEndTime, format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
            col(main_menu_col).alias('main_menu')
        ).cache()
        
        menu_metrics = base_df.groupBy(
            month(col('ChatEndTime')).alias('month'),
            col('main_menu')
        ).agg(
            count(col('RoomCode')).alias('main_menu_chat_counts')
        )
        
        export_data(menu_metrics, "output/menu_metrics",export_path,last_month,clientid)
        base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Optimized drops analysis
def drops(main_df,clientid,export_path,last_month):
    logger.info(f"Processing drops metrics for month {last_month}")
    
    base_df = main_df.select(
        main_df.RoomCode,
        main_df.RecipeFlow,
        to_timestamp(main_df.ChatEndTime, format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
    ).filter(
        (main_df.ClosedBy == 'System') & 
        (month(col('ChatEndTime')) == last_month)
    ).cache()
    
    drops_metrics = base_df.groupBy(
        element_at(split(col('RecipeFlow'), '->'), -1).alias('drops_at')
    ).agg(
        count(col('RoomCode')).alias('total_chats')
    )
    
    export_data(drops_metrics, "output/drops_metrics",export_path,last_month,clientid)
    base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Change here - Agent Metrics - Last 6 Months

def agent_metrics(main_df,clientid,export_path,last_month):
    logger.info("Processing agent metrics")
    
    base_df = main_df.filter(
        (main_df.RoomStatus == 'RESOLVED') & 
        (~((main_df.ClosedBy == 'System') | (main_df.ClosedBy == 'Dina')))
    ).select(
        to_timestamp(main_df.ChatEndTime, format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
        main_df.FirstAgentFirstResponseTime,
        main_df.TotalAgentResponseTime,
        main_df.AverageAgentResponseTime,
        col('RoomCode')
    ).cache()
    
    # Convert time strings to seconds in a single pass
    time_metrics = base_df.withColumn(
        'FirstAgentFirstResponseTime_sec',
        (
            element_at(split(col('FirstAgentFirstResponseTime'), ':'), 1).cast('int') * 3600 +
            element_at(split(col('FirstAgentFirstResponseTime'), ':'), 2).cast('int') * 60 +
            element_at(split(col('FirstAgentFirstResponseTime'), ':'), 3).cast('int')
        )
    ).withColumn(
        'TotalAgentResponseTime_sec',
        (
            element_at(split(col('TotalAgentResponseTime'), ':'), 1).cast('int') * 3600 +
            element_at(split(col('TotalAgentResponseTime'), ':'), 2).cast('int') * 60 +
            element_at(split(col('TotalAgentResponseTime'), ':'), 3).cast('int')
        )
    ).withColumn(
        'AverageAgentResponseTime_sec',
        (
            element_at(split(col('AverageAgentResponseTime'), ':'), 1).cast('int') * 3600 +
            element_at(split(col('AverageAgentResponseTime'), ':'), 2).cast('int') * 60 +
            element_at(split(col('AverageAgentResponseTime'), ':'), 3).cast('int')
        )
    )
    
    agent_metrics = time_metrics.groupBy(
        month(col('ChatEndTime')).alias('month')
    ).agg(
        round(avg(col('FirstAgentFirstResponseTime_sec')), 1).alias('avg_FirstAgentFirstResponseTime_sec'),
        round(avg(col('TotalAgentResponseTime_sec')), 1).alias('avg_TotalAgentResponseTime_sec'),
        round(avg(col('AverageAgentResponseTime_sec')), 1).alias('AverageAgentResponseTime_sec')
    )
    
    export_data(agent_metrics, "output/agent_metrics",export_path,last_month,clientid)
    base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Change here - Recipe - Last 6 Months

def recipe(main_df,clientid,export_path,last_month):
    logger.info("Processing recipe metrics")
    
    base_df = main_df.filter(
        (main_df.RoomStatus == 'RESOLVED')
    ).select(
        main_df.RoomCode,
        main_df.RecipeId,
        main_df.RecipeName,
        to_timestamp(main_df.ChatEndTime, format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime')
    ).cache()
    
    recipe_metrics = base_df.groupBy(
        month(col('ChatEndTime')).alias('month'),
        col('RecipeName')
    ).agg(
        count(col('RoomCode')).alias('total_chats')
    )
    
    export_data(recipe_metrics, "output/recipe_metrics",export_path,last_month,clientid)
    base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Change here - Bot Closure - Last 1 Month

def bot_closure(main_df,clientid,export_path,last_month):
    logger.info(f"Processing bot closure metrics for month {last_month}")
    
    base_df = main_df.filter(
        (main_df.RoomStatus == 'RESOLVED') &
        (main_df.ClosedBy == 'System') &
        (month(to_timestamp(main_df.ChatEndTime, format='dd-MM-yyyy HH:mm:ss')) == last_month)
    ).select(
        main_df.RoomCode,
        main_df.ClosingComment
    ).cache()
    
    closure_metrics = base_df.groupBy(
        col('ClosingComment').alias('closing_comment')
    ).agg(
        count(col('RoomCode')).alias('total_chats')
    )
    
    export_data(closure_metrics, "output/bot_closure_metrics",export_path,last_month,clientid)
    base_df.unpersist()

# ----------------------------------------------------------------------------------------
# Optimized main entry point
def newsletter_entry():
    parser = argparse.ArgumentParser(description='Generate Verloop Monthly Newsletter Analytics')
    # clientid = whatsapptesting.mena/express
    parser.add_argument('--clientid', required=True, help='Client ID')
    # base-path where csv files are present [s3://verloop-reports-cloudxero/files/]
    # after files/ all csv files are present
    parser.add_argument('--base-path', required=True, help='Base path for input files') 
    # export path where csv files will be saved [s3://verloop-reports-cloudxero/files/]
    # Note: Parquet file will be saved like - s3://verloop-reports-cloudxero/files/output/<last_month>/<mertics name>/<something.parquet>
    # s3://verloop-reports-cloudxero/files/output/3/agent_metrics/part-00000-d3d5d0b8-70ef-4a00-a0b4-f211eb727425-c000.snappy.parquet
    parser.add_argument('--export-path', required=True, help='Base path for input files') 
    # Last month number [3,5,12,1 like this]
    parser.add_argument('--last-month', type=int, help='Last month to analyze (1-12)')
    args = parser.parse_args()
    
    # Set default last month to previous month if not specified
    if not args.last_month:
        last_month = (datetime.now() - timedelta(days=30)).month
    else:
        last_month = args.last_month
    
    logger.info(f"Starting analytics for client {args.clientid}")
    logger.info(f"Processing data from {args.base_path}")
    logger.info(f"Analyzing month: {last_month}")
    
    spark = intiate_spark_connection()
    clientid = args.clientid
    base_path = args.base_path
    export_path = args.export_path
    last_month = args.last_month

    try:
        # main_df = read_files(spark, file_list(args.base_path))
        main_df = read_files(spark, args.base_path)
        
        # Process all metrics
        chat_metrics(main_df,clientid,export_path,last_month)
        channels(main_df,clientid,export_path,last_month)
        peak_hour(main_df,clientid,export_path,last_month)
        csat_metrics(main_df,clientid,export_path,last_month)
        menu_metrics(main_df,clientid,export_path,last_month)
        drops(main_df,clientid,export_path,last_month)
        agent_metrics(main_df,clientid,export_path,last_month)
        recipe(main_df,clientid,export_path,last_month)
        bot_closure(main_df,clientid,export_path,last_month)
        
        logger.info("All metrics processed successfully")
        
    except Exception as e:
        logger.error(f"Error processing metrics: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    newsletter_entry()
    


