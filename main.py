import mysql.connector
import pandas as pd
import json
from datetime import datetime
import smtplib
import logging
from email.message import EmailMessage
from urllib.parse import urlparse

current_datetime = datetime.now().strftime("%d %b %Y")

logging.basicConfig(
    filename='logs.log',
    level=logging.INFO,
    filemode='a',
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_config():
    try:
        with open("config.json",'r') as file:
            print("Loaded config")
            logging.info("Loaded Config")
            return json.load(file)

    except FileNotFoundError:
        print("Error: 'config.json' file not found.")
        logging.error("File not found")
        return None

def get_server_url_info(url):
    """Extract server URL information including path"""
    try:
        parsed_url = urlparse(url)
        # Return the base URL (protocol + domain + path)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        # Remove trailing slash if present
        if base_url.endswith('/'):
            base_url = base_url[:-1]
        return base_url
    except Exception as e:
        logging.error(f"URL parsing error: {e}")
        return "Unknown Server URL"


def create_db_connection(config):
    try:
        db = mysql.connector.connect(
            host=config.get("host"),
            user=config.get("user"),
            password=config.get('password'),
            database=config.get("database"),
        )
        logging.info("Connected to database")
        return db
    except Exception as e:
        logging.error(f"Failed to establish Connection due to: {e}")
        return None

def execute_query(query, db_connection):
    try:
        cursor = db_connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        return df
    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error
    finally:
        cursor.close()

# Updated send_mail function for AWS SES with server URL
def send_mail(html, config, server_url):
    msg = EmailMessage()
    msg['Subject'] = f'OpenSpecimen: Slow query report for {current_datetime}'
    msg['From'] = config.get('emailid')  # This should be dummy@gmail.com

    receiver_emails = config.get('to_emailid')
    if isinstance(receiver_emails, str):
        receiver_emails = [receiver_emails]
    msg['To'] = ", ".join(receiver_emails)

    msg.set_content(f'Please find below the report on queries run during the last 24 hours from {server_url}.')
    msg.add_alternative(html, subtype='html')
    
    try:
        # Updated SMTP configuration for AWS SES
        with smtplib.SMTP(config.get('smtp_server'), config.get('smtp_port', 587)) as smtp:
            smtp.starttls()  # AWS SES requires TLS
            # Use AWS SES SMTP credentials (not your AWS console credentials)
            smtp.login(config.get('smtp_username'), config.get('smtp_password'))
            smtp.send_message(msg)

        logging.info(f"Email Sent via AWS SES for {server_url}!!")
        print(f"Email sent successfully via AWS SES for {server_url}!")
        return True
    
    except smtplib.SMTPAuthenticationError as e:
        print(f"Authentication failed: {e}")
        print("Check your AWS SES SMTP username and password")
        logging.error(f"SMTP Authentication error: {e}")
        return False
    except smtplib.SMTPRecipientsRefused as e:
        print(f"Recipient refused: {e}")
        print("Make sure recipient email is verified in AWS SES (if in sandbox mode)")
        logging.error(f"Recipient refused: {e}")
        return False
    except smtplib.SMTPServerDisconnected as e:
        print(f"Server disconnected: {e}")
        logging.error(f"Server disconnected: {e}")
        return False
    except smtplib.SMTPException as e:
        print(f"SMTP error: {e}")
        logging.error(f"SMTP error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        logging.error(f"Unexpected error: {e}")
        return False

def get_query_url(query_ids, base_url):
    query_urls = {}
    if not query_ids:
        return query_urls
    
    for query_id in query_ids:
        if query_id is not None:
            query_url = f"{base_url}addedit?queryId={int(query_id)}"
            query_urls[query_id] = {
                'query_id':query_id,
                'url':query_url
            }
            print(f"Constructed url for query_id = {query_id} and {query_url}")
    logging.info("Extracted urls")
    return query_urls
                   
def add_url_to_dataframe(df, config):
    query_ids = df['query_id'].dropna().tolist()
    urls = get_query_url(query_ids, config.get('url'))
    
    df['URL'] = df['query_id'].apply(
    lambda qid: f'<a href="{urls[qid]["url"]}" target="_blank">Click here</a>' if qid in urls else None
    )
    logging.info('Added urls to the dataframe')
    return df

def generate_html_report(query, output_filename, config, db_connection, drop_columns=None):
    try:
        df = execute_query(query, db_connection)

        if 'query_id' in df.columns:
            df = add_url_to_dataframe(df, config)

        if drop_columns:
            missing_cols = [col for col in ([drop_columns] if isinstance(drop_columns, str) else drop_columns) if col not in df.columns]
            if missing_cols:
                logging.warning(f"Columns to drop not found: {missing_cols}")
            df = df.drop(columns=[col for col in drop_columns if col in df.columns], errors='ignore')
        
        df.columns = [col.replace('_', ' ').title() for col in df.columns]

        df.columns = [col.replace('Time Taken', 'Time Taken (in seconds)') for col in df.columns]
        df.columns = [col.replace('Cnt', 'Number of Queries') for col in df.columns]

        # Format Date column (existing functionality)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime("%d %b %Y %H:%M:%S")

        # Format Call Start Time column
        if 'Call Start Time' in df.columns:
            df['Call Start Time'] = pd.to_datetime(df['Call Start Time']).dt.strftime("%d %b %Y %H:%M:%S")

        # Format Call End Time column
        if 'Call End Time' in df.columns:
            df['Call End Time'] = pd.to_datetime(df['Call End Time']).dt.strftime("%d %b %Y %H:%M:%S")

        # Format Start Time column (from slow query log)
        if 'Start Time' in df.columns:
            df['Start Time'] = pd.to_datetime(df['Start Time']).dt.strftime("%d %b %Y %H:%M:%S")

        html_table = df.to_html(index=False, escape=False)

        with open(output_filename, 'w') as f:
            f.write(html_table)

        logging.info(f"Report generated: {output_filename}")
        return html_table

    except Exception as e:  
        logging.error(f"Failed to generate report for {output_filename}: {e}")
        return ""

def main():
    logging.info("Process Started")
    
    # Load configuration
    config = load_config()
    if config is None:
        logging.error("Failed to load configuration. Exiting.")
        return
    
    # Get server URL from config
    server_url = get_server_url_info(config.get('url', ''))
    logging.info(f"Server URL identified as: {server_url}")
    
    # Create database connection
    db_connection = create_db_connection(config)
    if not db_connection:
        logging.error("Cannot generate report due to failed DB connection. Exiting.")
        return
    
    try:
        queries = [
            {
                "title": "Top 5 slowest running queries.",
                "query": """SELECT 
                                logs.query_id,  
                                CONCAT(usr.first_name, ' ', usr.last_name) AS name,  
                                MAX(logs.time_of_exec) AS Date,  
                                MAX(logs.time_to_finish) AS time_taken  
                            FROM 
                                catissue_query_audit_logs logs  
                            INNER JOIN 
                                catissue_user usr ON logs.run_by = usr.identifier  
                            WHERE 
                                logs.query_id IS NOT NULL  
                                    AND logs.time_of_exec >= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 0 SECOND
                                    AND logs.time_of_exec <= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 1 DAY - INTERVAL 1 SECOND
                                AND query_sql NOT LIKE '%limit 0, 101%'
                            GROUP BY 
                                name, logs.query_id
                            ORDER BY 
                                time_taken DESC
                            LIMIT 5;""",
                "output_filename": "slowest_running_queries.html",
                "drop_columns": []
            },
            {
                "title": "Top 5 most run saved queries.",
                "query": """SELECT logs.query_id, 
                                   CONCAT(first_name, ' ', last_name) AS name, 
                                   COUNT(*) AS Count, 
                                   MAX(time_of_exec) AS Date, 
                                   MAX(time_to_finish) AS time_taken  
                            FROM catissue_query_audit_logs logs 
                            JOIN catissue_user user ON logs.run_by = user.identifier 
                            WHERE query_id IS NOT NULL 
                              AND logs.time_of_exec >= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 0 SECOND
                            AND logs.time_of_exec <= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 1 DAY - INTERVAL 1 SECOND
                            GROUP BY logs.query_id,name 
                            ORDER BY Count DESC 
                            LIMIT 5;""",
                "output_filename": "most_run_saved_queries.html",
                "drop_columns": ["time_taken"]
            },
            {
                "title": "Top 10 users running most queries.",
                "query": """SELECT 
                        CONCAT(first_name, ' ', last_name) AS name, 
                        COUNT(*) AS cnt, 
                        MAX(time_of_exec) AS Date,
                        MAX(time_to_finish) AS time_taken 
                    FROM 
                        catissue_query_audit_logs logs 
                    JOIN 
                        catissue_user user 
                        ON user.identifier = logs.run_by 
                    WHERE 
                           logs.time_of_exec >= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 0 SECOND
                            AND logs.time_of_exec <= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 1 DAY - INTERVAL 1 SECOND 
                        AND query_sql NOT LIKE '%limit 0, 101%'
                    GROUP BY 
                        run_by
                    ORDER BY 
                        cnt DESC
                    LIMIT 10;
                            """,
                "output_filename": "users_running_most_queries.html",
                "drop_columns": ["time_taken"]
            },
            {
                "title": "Top 5 Slow API Calls.",
                "query": """SELECT
                            CONCAT(usr.first_name, ' ', usr.last_name) AS Name,
                            logs.method,
                            logs.call_start_time,
                            logs.call_end_time,
                            logs.url,
                            TIMESTAMPDIFF(SECOND, logs.call_start_time, logs.call_end_time) AS "Time Taken"
                        FROM
                            os_user_api_calls_log logs
                        JOIN
                            catissue_user usr ON usr.identifier = logs.user_id
                        WHERE
                             logs.CALL_START_TIME >= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 0 SECOND
                            AND logs.CALL_START_TIME <= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 1 DAY - INTERVAL 1 SECOND
                        ORDER BY
                            TIMESTAMPDIFF(SECOND, logs.call_start_time, logs.call_end_time) DESC
                        LIMIT 5;
                        """,
                "output_filename": "slow_api_calls.html",
                "drop_columns": []
            }, 
            {
                "title": "Top 5 Slow Queries from slow query log.",
                "query": """SELECT
                        start_time AS "Start Time",
                        CONVERT(sql_text USING utf8) as Query,
                        TIME_TO_SEC(query_time) AS "Time Taken",
                        db AS "Database"
                    FROM
                        mysql.slow_log
                    WHERE
                             mysql.slow_log.start_time >= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 0 SECOND
                            AND  mysql.slow_log.start_time <= DATE(NOW() - INTERVAL 1 DAY) + INTERVAL 1 DAY - INTERVAL 1 SECOND
                    ORDER BY
                        query_time DESC
                    LIMIT 5;
                        """,
                "output_filename": "slow_logs.html",
                "drop_columns": []
            }
              
        ]

        html_sections = ""

        for q in queries:
            html_table = generate_html_report(
                query=q["query"],
                output_filename=q["output_filename"],
                config=config,
                db_connection=db_connection,
                drop_columns=q["drop_columns"]
            )
            html_sections += f"<h3>{q['title']}</h3>{html_table}<br>"

        combined_html = f"""
        <html>
        <body>
            <div style="background-color: #f0f8ff; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #007bff;">
                <h2 style="color: #007bff; margin: 0;">OpenSpecimen Query Report</h2>
                <p style="margin: 5px 0 0 0; color: #666;"><strong>Server:</strong> <a href="{server_url}" target="_blank" style="color: #007bff; text-decoration: none;">{server_url}</a></p>
                <p style="margin: 5px 0 0 0; color: #666;">Report generated on {datetime.now().strftime("%d %b %Y %H:%M:%S")}</p>
            </div>
            
            <p>Please find below the report on queries run during the last 24 hours from <strong><a href="{server_url}" target="_blank" style="color: #007bff; text-decoration: none;">{server_url}</a></strong>.</p>
            {html_sections}
            
            <div style="margin-top: 30px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; border-top: 3px solid #28a745;">
                <p><strong>Thanks,</strong><br>OpenSpecimen Administrator</p>
                <strong>Report Time:</strong> {datetime.now().strftime("%d %b %Y %H:%M:%S")}</small></p>
            </div>
        </body>
        </html>
        """

        email_sent = send_mail(combined_html, config, server_url)

    except Exception as e:
        logging.error(e)
    finally:
        # Close database connection
        if db_connection:
            db_connection.close()
            logging.info("Database connection closed")

if __name__ == '__main__':
    main()