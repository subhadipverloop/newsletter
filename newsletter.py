# change here - All imports are defined here
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_timestamp,month,count,\
                                    size,count_distinct,when,hour,weekday, \
                                    split,element_at,\
                                    avg,round
import os, re, argparse
from hurry.filesize import size
# ----------------------------------------------------------------------------------------
#### Logging Pending

# ----------------------------------------------------------------------------------------
# change here - Initating spark session
# Default using all cores
# 100% CPU utilization while running Stay Calm

def intiate_spark_connection():
    spark = SparkSession.builder\
        .appName('Starting Pyspark Application for Verloop Monthly Newsletter :)')\
        .getOrCreate()
    return spark

# ----------------------------------------------------------------------------------------
# Export incoming data lamda function - PENDING

def export_data(final_df):
    print(final_df.show())

# ----------------------------------------------------------------------------------------
# Pending - Only for local
def file_list(base_path):
    files = []
    for each in os.listdir(base_path):
        files.append(f'{base_path}\\{each}')
    for f in files:
        print(f'File Size - {size(os.path.getsize(f))} | File to be processed - {f}')
    return files

# ----------------------------------------------------------------------------------------
# Read all files from the path

def read_files(spark,file_list):
    main_df = spark.read\
        .option("header",True)\
        .option('multiline', True)\
        .csv(file_list)
    return main_df

# ----------------------------------------------------------------------------------------
# Change here - Chat_Metrics - Last 6 Months

def chat_metrics(main_df):
    df = main_df.filter(
            (main_df.RoomStatus=='RESOLVED')
        ).select(
            main_df.RoomCode,
            to_timestamp(main_df.ChatEndTime,format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
            main_df.UserId,
            main_df.ClosedBy
        )
    # ---------------------
    # ---------------------
    # ---------------------
    d0 = df.groupBy(
        month(df.ChatEndTime).alias("month")
        ).agg(
            count(df.RoomCode).alias("incoming_chats"),
            count_distinct(df.UserId).alias('unique_users')
        )

    d1 = df.groupBy(
            month(df.ChatEndTime).alias('month_'),
            df.UserId.alias('user_id')
        ).agg(
            count(df.RoomCode).alias('incoming_chats'),
        ).filter(
            col("incoming_chats")>1
        ).groupBy(
            col('month_')
        ).agg(
            count(col('user_id')).alias('recurring_users')
        )
    d0= d0.join(d1,d0.month == d1.month_,"inner")
    chat_metrics_1 = d0.select(d0.month,d0.incoming_chats,d0.unique_users,d0.recurring_users)
    # chat_metrics_1.show(10)
    export_data(chat_metrics_1)
    # ---------------------
    # ---------------------
    # ---------------------
    chat_metrics_2 = df.groupBy(
            month(df.ChatEndTime).alias("month")
        ).agg(
            count(col('RoomCode')).alias('total_room_count'),
            count(
                when(
                    (col('ClosedBy') == 'System'),
                    df.RoomCode
                )
            ).alias('closed_by_bot'),
            count(
                when(
                    (col('ClosedBy') != 'System'),
                    df.RoomCode  
                )
            ).alias('closed_by_agent')
        )
    # chat_metrics_2.show(10)
    export_data(chat_metrics_2)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export pending

# ----------------------------------------------------------------------------------------
# Change here - Channels/Channel Metric/Peak Hour Hourly Volume - Last 1 Month 

def channels(main_df, last_month):
    df = main_df.select(
        main_df.RoomCode,
        to_timestamp(main_df.ChatEndTime,format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
        main_df.UserOs,
        main_df.ChatChannel,
        ).filter(
            (main_df.RoomStatus=='RESOLVED') & 
            (month(col('ChatEndTime')) == last_month)
        )
    # ---------------------
    # ---------------------
    # ---------------------
    channels_1 = df.groupBy(
            hour(df.ChatEndTime).alias('hour')
        ).agg(
            count(df.RoomCode).alias('total_chats'),
            count(when(
                col('ChatChannel')=='LIVECHAT',df.RoomCode
            )).alias('channel_livechat'),
            count(when(
                col('ChatChannel') == 'WHATSAPP',df.RoomCode
            )).alias('channel_whatsapp'),
            count(when(
                ~(
                    (col('ChatChannel') == 'LIVECHAT') |
                    (col('ChatChannel') == 'WHATSAPP')
                ),df.RoomCode
            )).alias('channel_other'),
            count(when(
                col('UserOs').rlike('^Android*'),df.RoomCode
            )).alias('device_android'),
            count(when(
                col('UserOs').rlike('^iOS*'),df.RoomCode
            )).alias('device_ios'),
            count(when(
                col('UserOs').rlike('^macOS*'),df.RoomCode
            )).alias('device_macos'),
            count(when(
                col('UserOs').rlike('^Windows*'),df.RoomCode
            )).alias('device_windows'),
            count(when(
                ~(
                    col('UserOs').rlike('^Android*') |
                    col('UserOs').rlike('^iOS*') |
                    col('UserOs').rlike('^macOS*') |
                    col('UserOs').rlike('^Windows*')
                ),df.RoomCode
            )).alias('device_other'),
        )
    # channels_1.show(10)
    export_data(channels_1)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - Peak Hour - Last 1 Month
# For hourly chat count refer to channels data - defined above

def peak_hour(main_df,last_month):
    df = main_df.select(
        main_df.RoomCode,
        to_timestamp(main_df.ChatEndTime,format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
        ).filter(
            (main_df.RoomStatus=='RESOLVED') & 
            (month(col('ChatEndTime')) == last_month)
        )
    # ---------------------
    # ---------------------
    # ---------------------
    # weekday [0-6] = [Monday-Sunday]
    peak_hour_2 = df.groupBy(
            weekday(df.ChatEndTime).alias('weekday')
        ).agg(
            count(df.RoomCode).alias('total_chats')
        )
    # peak_hour_2.show(10)
    export_data(peak_hour_2)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - CSAT Metrics - Last 6 Months

def csat_metrics(main_df):
    df = main_df.select(
        main_df.RoomCode,
        to_timestamp(main_df.CsatSubmittedAt,format='dd-MM-yyyy HH:mm:ss').alias('csat_submittedat'),
        to_timestamp(main_df.CsatTriggeredAt,format='dd-MM-yyyy HH:mm:ss').alias('csat_triggeredat'),
        main_df.UserId,
        main_df.ClosedBy
    ).filter(col('csat_triggeredat').isNotNull())
    # ---------------------
    # ---------------------
    # ---------------------
    d0 = df.groupBy(
        month(col('csat_triggeredat')).alias('month')
        ).agg(
            count(col('csat_triggeredat')).alias('total_triggered_csat'),
            count(
                when(
                    col('csat_submittedat').isNotNull(),
                    col('csat_submittedat')
                )
            ).alias('total_submitted_csat'),
            count_distinct(
                when(
                    col('csat_submittedat').isNotNull(),
                    col('UserId')
                )
            ).alias('unique_user_submitted_csat'),
            count(
                when(
                    (
                        col('csat_submittedat').isNotNull() &
                        (col('ClosedBy') == 'System')
                    ),
                    col('csat_submittedat')
                )
            ).alias('bot_submitted_csat'),
            count(
                when(
                    (
                        col('csat_submittedat').isNotNull() &
                        (col('ClosedBy') != 'System')
                    ),
                    col('csat_submittedat')
                )
            ).alias('agent_submitted_csat'),

        )
    d1 = df.filter(
            col('csat_submittedat').isNotNull() 
        ).groupBy(
            month(col('csat_triggeredat')).alias('month_'),
            col('UserId')
        ).agg(
            count(
                col('csat_submittedat')
            ).alias('recurring_user_submitted_csat_'),
        ).filter(
            col('recurring_user_submitted_csat_')>1
        ).groupBy(
            col('month_').alias('month')
        ).agg(
            count(
                col('recurring_user_submitted_csat_')
            ).alias('recurring_user_submitted_csat')
        )
    csat_metrics_1 = d0.join(d1,d0.month == d1.month,'inner').select(
            d0.month,
            col('total_triggered_csat'),
            col('total_submitted_csat'),
            col('unique_user_submitted_csat'),
            col('recurring_user_submitted_csat'),
            col('bot_submitted_csat'),
            col('agent_submitted_csat')
        )
    # csat_metrics_1.show(10)
    export_data(csat_metrics_1)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - Menu_Metrics - Last 6 Months

def menu_metrics(main_df):
    #### Change here - To change/add main menu block
    main_manu = ['mainmenu', 'main_menu','main-menu']
    ####
    main_menu_block = None
    for each in main_df.columns:
        if each.lower() in main_manu:
            main_menu_block = each
            break
    # ---------------------
    # ---------------------
    # ---------------------
    if main_menu_block:
        df = main_df.filter(
            (main_df.RoomStatus=='RESOLVED')
        ).select(
            main_df.RoomCode,
            to_timestamp(main_df.ChatEndTime,format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
            col(main_menu_block).alias('main_menu'),
        )

        main_menu_metrics = df.filter(
                col('main_menu').isNotNull()
            ).groupBy(
                month(col('ChatEndTime')).alias('month'),
                col('main_menu')
            ).agg(
                count(col('RoomCode')).alias('main_menu_chat_counts')
            )
        # main_menu_metrics.show(10)
        export_data(main_menu_metrics)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - Drops - Last 1 Month

def drops(main_df,last_month):
    df = main_df.select(
        main_df.RoomCode,
        main_df.RecipeFlow,
        to_timestamp(main_df.ChatEndTime,format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
        ).filter(
            (main_df.ClosedBy=='System') & 
            (month(col('ChatEndTime')) == last_month)
        )
    # ---------------------
    # ---------------------
    # ---------------------
    drops = df.groupBy(
            element_at(
                split(
                    col('RecipeFlow'),
                    '->'
                ),
                -1
            ).alias('drops_at')
        ).agg(
            count(col('RoomCode')).alias('total_chats')
        )
    # drops.show(10)
    export_data(drops)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - Agent Metrics - Last 6 Months

def agent_metrics(main_df):
    df = main_df.filter(
            (main_df.RoomStatus == 'RESOLVED') & 
            (~((main_df.ClosedBy == 'System') | (main_df.ClosedBy == 'Dina')))
        ).select(
            to_timestamp(main_df.ChatEndTime,format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime'),
            main_df.FirstAgentFirstResponseTime,
            main_df.TotalAgentResponseTime,
            main_df.AverageAgentResponseTime,
            col('RoomCode')
        )
    # ---------------------
    # ---------------------
    # ---------------------
    # Convrting String duration in Seconds (Example: 00:05:31 = 331)
    # values are in seconds
    df = df.withColumn(
        'FirstAgentFirstResponseTime_sec',
        (
            (
                element_at(
                    split(
                        col('FirstAgentFirstResponseTime'),
                        ':'
                    ),
                    1
                ).cast('int')  * 3600
            ) +
            (
                element_at(
                    split(
                        col('FirstAgentFirstResponseTime'),
                        ':'
                    ),
                    2
                ).cast('int')  * 60 
            )+
            (
                element_at(
                    split(
                        col('FirstAgentFirstResponseTime'),
                        ':'
                    ),
                    3
                ).cast('int') 
            )
        )
        ).withColumn(
        'TotalAgentResponseTime_sec',
        (
            (
                element_at(
                    split(
                        col('TotalAgentResponseTime'),
                        ':'
                    ),
                    1
                ).cast('int')  * 3600
            ) +
            (
                element_at(
                    split(
                        col('TotalAgentResponseTime'),
                        ':'
                    ),
                    2
                ).cast('int')  * 60 
            )+
            (
                element_at(
                    split(
                        col('TotalAgentResponseTime'),
                        ':'
                    ),
                    3
                ).cast('int') 
            )
        ) 
        ).withColumn(
        'AverageAgentResponseTime_sec',
        (
            (
                element_at(
                    split(
                        col('AverageAgentResponseTime'),
                        ':'
                    ),
                    1
                ).cast('int')  * 3600
            ) +
            (
                element_at(
                    split(
                        col('AverageAgentResponseTime'),
                        ':'
                    ),
                    2
                ).cast('int')  * 60 
            )+
            (
                element_at(
                    split(
                        col('AverageAgentResponseTime'),
                        ':'
                    ),
                    3
                ).cast('int') 
            )
        )     
        )
    agent_metrics = df.groupBy(
        month(col('ChatEndTime')).alias('month')
        ).agg(
            round(avg(col('FirstAgentFirstResponseTime_sec')),1).alias('avg_FirstAgentFirstResponseTime_sec'),
            round(avg(col('TotalAgentResponseTime_sec')),1).alias('avg_TotalAgentResponseTime_sec'),
            round(avg(col('AverageAgentResponseTime_sec')),1).alias('AverageAgentResponseTime_sec')
        )
    # agent_metrics.show(10)
    export_data(agent_metrics)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - Recipe - Last 6 Months

def recipe(main_df):
    df = main_df.filter(
            (main_df.RoomStatus == 'RESOLVED')
        ).select(
            main_df.RoomCode,
            main_df.RecipeId,
            main_df.RecipeName,
            to_timestamp(main_df.ChatEndTime,format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime')
        )
    # ---------------------
    # ---------------------
    # ---------------------
    recipe = df.groupBy(
            month(col('ChatEndTime')).alias('month'),
            df.RecipeName
        ).agg(
            count(df.RoomCode).alias('total_chats')
        )
    # recipe.show(10)
    export_data(recipe)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - Bot Closure - Last 1 Month

def bot_closure(main_df,last_month):
    df = main_df.filter((main_df.RoomStatus == 'RESOLVED')).select(
            main_df.RoomCode,
            main_df.ClosingComment,
            to_timestamp(main_df.ChatEndTime,format='dd-MM-yyyy HH:mm:ss').alias('ChatEndTime')
        ).filter(
            (main_df.ClosedBy=='System') & 
            (month(col('ChatEndTime')) == last_month)
        )
    # ---------------------
    # ---------------------
    # ---------------------
    bot_closure = df.groupBy(
            col('ClosingComment').alias('closing_comment')
        ).agg(
            count(col('RoomCode').alias('total_chats'))
        )
    # bot_closure.show(10)
    export_data(bot_closure)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - CSAT - Last 6 Months

def csat(main_df):
    df = main_df.select(
        main_df.RoomCode,
        to_timestamp(main_df.CsatSubmittedAt,format='dd-MM-yyyy HH:mm:ss').alias('csat_submittedat'),
        to_timestamp(main_df.CsatTriggeredAt,format='dd-MM-yyyy HH:mm:ss').alias('csat_triggeredat'),
        main_df.CsatScore
    ).filter(col('csat_triggeredat').isNotNull())
    # ---------------------
    # ---------------------
    # ---------------------
    csat1 = df.groupBy(
            month(col('csat_triggeredat')).alias('month')
        ).agg(
            round(
                avg(
                    when(
                        col('csat_submittedat').isNotNull(),
                        col('CsatScore')
                    )
                )*20,
                2
            ).alias('normalized_csat'),
            count(
                when(
                    col('csat_submittedat').isNotNull(),
                    col('csat_submittedat')
                    )
                ).alias('total_csat_submitted'),
            count(
                col('csat_triggeredat')
                ).alias('total_csat_triggered')
        )
    # csat.show(10)
    export_data(csat1)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------
# Change here - CSAT 2 - Last 6 Months

def csat_2(main_df):
    df = main_df.select(
        main_df.RoomCode,
        to_timestamp(main_df.CsatSubmittedAt,format='dd-MM-yyyy HH:mm:ss').alias('csat_submittedat'),
        main_df.CsatScore,
        main_df.ClosedBy
    ).filter(col('csat_submittedat').isNotNull())
    # ---------------------
    # ---------------------
    # ---------------------
    csat2 = df.groupBy(
            month(col('csat_submittedat')).alias('month')
        ).agg(
            round(
                avg(
                    when(
                        col('ClosedBy')=='System',
                        col('CsatScore')
                    )
                )*20,
                2
            ).alias('normalized_bot_csat'),
            round(
                avg(
                    when(
                        col('ClosedBy')!='System',
                        col('CsatScore')
                    )
                )*20,
                2
            ).alias('normalized_agent_csat'),
        )
    # csat2.show(10)
    export_data(csat2)
    # ---------------------
    # ---------------------
    # ---------------------
    # Export Pending

# ----------------------------------------------------------------------------------------

# Pending task - Create unified function to collect and send final result 
# from each function to serverless function to update g sheet
#

def newsletter_entry():
    ## change as per requirement
    ## Pending collect this as cli argument
    clientid = ''
    base_storage_path = ''
    base_path = 'M:\\verloop\\DA\\adib'
    last_month = 3
    spark = intiate_spark_connection()
    main_df = read_files(
            spark=spark,
            file_list=file_list(base_path=base_path)
        )
    chat_metrics(main_df=main_df)
    channels(main_df=main_df,last_month=last_month)
    peak_hour(main_df=main_df,last_month=last_month)
    csat_metrics(main_df=main_df)
    menu_metrics(main_df=main_df)
    drops(main_df=main_df,last_month=last_month)
    agent_metrics(main_df=main_df)
    recipe(main_df=main_df)
    bot_closure(main_df=main_df,last_month=last_month)
    csat(main_df=main_df)
    csat_2(main_df=main_df)
newsletter_entry()
    



