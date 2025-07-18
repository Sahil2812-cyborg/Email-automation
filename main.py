import mysql.connector
import pandas as pd
import json
from datetime import datetime
import smtplib
import logging
from email.message import EmailMessage

current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

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
        return
    
config = load_config()

host = config.get("host")
user = config.get("user")
password = config.get('password')
database = config.get("database")
emailid = config.get('emailid')
email_password = config.get('emailpassword')
to_emailid = config.get('to_emailid')
base_url = config.get('url')


def create_db_connection():
    try:
        db = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
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



def send_mail(html):
    msg = EmailMessage()
    msg['Subject'] = f'Query Report: Top Slow & Most Run Querie for {current_datetime}'
    msg['From'] = emailid
    msg['To'] = to_emailid

    msg.set_content('Please find below the report containing the top 5 slow-running queries, most frequently run saved queries, and users executing the highest number of queries in the last 24 hours.')
    msg.add_alternative(html, subtype='html')
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
            smtp.starttls()
            smtp.login(emailid, email_password)
            smtp.send_message(msg)

        logging.info("Email Sent!!")
        return True
    
    except smtplib.SMTPAuthenticationError as e:
        print(f"Authentication failed: {e}")
        print("Check your email and password/app password")
        logging.error(f"Authentication error: {e}")
        return False
    except smtplib.SMTPRecipientsRefused as e:
        print(f"Recipient refused: {e}")
        logging.error((f"Recipient refused: {e}"))
        return False
    except smtplib.SMTPServerDisconnected as e:
        print(f"Server disconnected: {e}")
        logging.error((f"Server disconnected: {e}"))
        return False
    except smtplib.SMTPException as e:
        print(f"SMTP error: {e}")
        logging.error(f"SMTP error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        logging.error(f"Unexpected error: {e}")
        return False

def get_query_url(query_ids, base_url=base_url):
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
                   

def add_url_to_dataframe(df):
    query_ids = df['query_id'].dropna().tolist()
    urls = get_query_url(query_ids)
    
    df['URL'] = df['query_id'].apply(lambda qid: urls[qid]['url'] if qid in urls else None)
    logging.info('Added urls to the dataframe')
    return df

def generate_html_report(query, output_filename, drop_columns=None):
    db_connection = create_db_connection()
    if not db_connection:
        logging.error("Cannot generate report due to failed DB connection.")
        return ""

    try:
        df = execute_query(query, db_connection)
        db_connection.close()

        if 'query_id' in df.columns:
            df = add_url_to_dataframe(df)

        if drop_columns:
            missing_cols = [col for col in ([drop_columns] if isinstance(drop_columns, str) else drop_columns) if col not in df.columns]
            if missing_cols:
                logging.warning(f"Columns to drop not found: {missing_cols}")
            df = df.drop(columns=[col for col in drop_columns if col in df.columns], errors='ignore')
        
        df.columns = [col.replace('_', ' ').title() for col in df.columns]


        html_table = df.to_html(index=False)

        with open(output_filename, 'w') as f:
            f.write(html_table)

        logging.info(f"Report generated: {output_filename}")
        return html_table

    except Exception as e:
        logging.error(f"Failed to generate report for {output_filename}: {e}")
        return ""


def main():
    logging.info("Process Started")
    try:
        queries = [
            {
                "title": "Top 5 slowest running queries.",
                "query": """SELECT logs.query_id, 
                                   CONCAT(first_name, ' ', last_name) AS user, 
                                   time_to_finish AS time_taken, 
                                   time_of_exec AS Date 
                            FROM catissue_query_audit_logs logs 
                            JOIN catissue_user usr ON logs.run_by = usr.identifier 
                            WHERE time_of_exec >= NOW() - INTERVAL 1 DAY 
                            ORDER BY time_to_finish DESC 
                            LIMIT 5;""",
                "output_filename": "slowest_running_queries.html",
                "drop_columns": []
            },
            {
                "title": "Top 5 most run saved queries.",
                "query": """SELECT logs.query_id, 
                                   CONCAT(first_name, ' ', last_name) AS name, 
                                   COUNT(*) AS cnt, 
                                   MAX(time_of_exec) AS Date, 
                                   MAX(time_to_finish) AS time_taken  
                            FROM catissue_query_audit_logs logs 
                            JOIN catissue_user user ON logs.run_by = user.identifier 
                            WHERE query_id IS NOT NULL 
                              AND time_of_exec >= NOW() - INTERVAL 1 DAY 
                            GROUP BY name, logs.query_id 
                            ORDER BY cnt DESC 
                            LIMIT 5;""",
                "output_filename": "most_run_saved_queries.html",
                "drop_columns": ["cnt"]
            },
            {
                "title": "Top 5 users running most queries.",
                "query": """SELECT CONCAT(first_name, ' ', last_name) AS name, 
                                   COUNT(*) AS cnt, 
                                   MAX(time_of_exec) AS Date, 
                                   MAX(time_to_finish) AS time_taken 
                            FROM catissue_query_audit_logs logs 
                            JOIN catissue_user user ON user.identifier = logs.run_by 
                            WHERE time_of_exec >= NOW() - INTERVAL 1 DAY 
                            GROUP BY run_by 
                            LIMIT 5;""",
                "output_filename": "users_running_most_queries.html",
                "drop_columns": ["cnt"]
            }
        ]

        html_sections = ""

        for q in queries:
            print(f"=========== {q['title']} ===========")
            html_table = generate_html_report(
                query=q["query"],
                output_filename=q["output_filename"],
                drop_columns=q["drop_columns"]
            )
            html_sections += f"<h3>{q['title']}</h3>{html_table}<br>"

        combined_html = f"""
        <html>
        <body>
            <p>Please find below the automated report containing the top 5 slow-running queries, most frequently run saved queries, and users executing the highest number of queries in the last 24 hours.</p>
            {html_sections}
            <p>The goal of this report is to help identify potential performance bottlenecks and active usage patterns.</p>
            <div style="margin-top: 30px; padding: 10px; background-color: #f8f9fa; border-radius: 5px;">
                <p><strong>Regards,</strong><br>Your Database Monitoring Script</p>
                <p><small>Report generated automatically on {current_datetime}</small></p>
            </div>
        </body>
        </html>
        """

        email_sent = send_mail(combined_html)

    except Exception as e:
        logging.error(e)


if __name__ == '__main__':
    main()
