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



def create_db_connection(query):
    try:
        db = mysql.connector.connect(
            host = host,
            user = user,
            password  = password,
            database = database,
        )

        cursor = db.cursor()
        logging.info("Connected to database")
    except Exception as e:
        logging.error(f"Failed to establish Connection due to: {e}")

    cursor.execute(query)
    rows = cursor.fetchall()
    cols = []

    for i in cursor.description:
        cols.append(i[0])

    df = pd.DataFrame(rows, columns=cols)

    print(df)
    cursor.close()
    db.close()
    return df


def send_mail(html):
    msg = EmailMessage()
    msg['Subject'] = f'Query Report for {current_datetime}'
    msg['From'] = emailid
    msg['To'] = to_emailid

    msg.set_content('This email contains the nightly database report.')
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
    
    df['url'] = df['query_id'].apply(lambda qid: urls[qid]['url'] if qid in urls else None)
    logging.info('Added urls to the dataframe')
    return df


def generate_html_report(query, output_filename, drop_columns=None):
    try:
        df = create_db_connection(query)

        if 'query_id' in df.columns:
            df = add_url_to_dataframe(df)
        try:
            if drop_columns and drop_columns in df.columns:
                df = df.drop(drop_columns, axis=1)
        except Exception as e:
            logging.error("Column doesn't exist: {e}: {drop_columns}")

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
        top_5_slowest_running_queries = 'select logs.query_id, concat(first_name,\' \',last_name) as user, time_to_finish as time_taken,time_of_exec as Date from catissue_query_audit_logs logs join catissue_user usr on logs.run_by=usr.identifier order by time_to_finish desc limit 5;'
        top_5_most_run_saved_queries = 'select logs.query_id,concat(first_name,\' \',last_name) as name, count(*) as cnt, Max(time_of_exec) as Date, max(time_to_finish) as time_taken  from catissue_query_audit_logs logs join catissue_user user on logs.run_by = user.identifier where query_id is not null group by name,logs.query_id order by cnt desc limit 5;'
        top_5_users_running_most_queries = 'select concat(first_name,\' \',last_name) as name, count(*) as cnt, Max(time_of_exec) as Date, max(time_to_finish) as time_taken from catissue_query_audit_logs logs join catissue_user user on user.identifier = logs.run_by group by run_by limit 5;'
        
        print('============Top 5 slow running queries================')
        slowest_running_queries_html_table = generate_html_report(
            query=top_5_slowest_running_queries,
            output_filename='slowest_running_queries.html'        
        )

        print('===========Top 5 most run saved queries=================')
        most_run_saved_queries_html_table = generate_html_report(
            query=top_5_most_run_saved_queries,
            output_filename='most_run_saved_queries.html',
            drop_columns=['cnt']       
             )

        print('===========Top 5 users running most queries=================')
        users_running_most_queries_html_table = generate_html_report(
            query=top_5_users_running_most_queries,
            output_filename='users_running_most_queries.html',
            drop_columns=['cnt']
)

        
        combined_html = f"""
        <html>
        <body>
            <p>Hi Team,</p>
            <p>Here is the nightly report:</p>
            <h3>Top 5 slowest running queries.</h3>
            {slowest_running_queries_html_table}
            <br>
            <h3>Top 5 most run saved queries.</h3>
            {most_run_saved_queries_html_table}
            <br>
            <h3>Top 5 users running most queries</h3>
            {users_running_most_queries_html_table}
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
