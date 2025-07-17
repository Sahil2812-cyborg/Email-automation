import mysql.connector
import pandas as pd
import json
from datetime import datetime
import smtplib
from email.message import EmailMessage

current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

def load_config():
    try:
        with open("config.json",'r') as file:
            print("Loaded config")
            return json.load(file)

    except FileNotFoundError:
        print("Error: 'config.json' file not found.")
        return
    
config = load_config()

host = config["host"]
user = config["user"]
password = config['password']
database = config["database"]


def create_connection(query):
    db = mysql.connector.connect(
        host = host,
        user = user,
        password  = password,
        database = database,
    )

    cursor = db.cursor()
    print('Connection Successful')

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
    msg['Subject'] = 'Nightly Report'
    msg['From'] = 'sahilratnaparkhi1@gmail.com'
    msg['To'] = 'sahil@krishagni.com'

    msg.set_content('This email contains the nightly database report.')
    msg.add_alternative(html, subtype='html')
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
            smtp.starttls()
            smtp.login('sahilratnaparkhi1@gmail.com', 'wodt opdh mlei lazm')
            smtp.send_message(msg)

        print("Email sent!")
        return True
    
    except smtplib.SMTPAuthenticationError as e:
        print(f"Authentication failed: {e}")
        print("Check your email and password/app password")
        return False
    except smtplib.SMTPRecipientsRefused as e:
        print(f"Recipient refused: {e}")
        return False
    except smtplib.SMTPServerDisconnected as e:
        print(f"Server disconnected: {e}")
        return False
    except smtplib.SMTPException as e:
        print(f"SMTP error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def get_query_url(query_ids, base_url="http://localhost:8080/openspecimen/#/queries/"):
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
    return query_urls
                   

def add_url_to_dataframe(df):
    query_ids = df['query_id'].dropna().tolist()
    urls = get_query_url(query_ids)
    
    df['url'] = df['query_id'].apply(lambda qid: urls[qid]['url'] if qid in urls else None)

    print("This is the df with url")
    print(df)

    return df


def main():
    top_5_slowest_running_queries = 'select logs.query_id, concat(first_name,\' \',last_name) as user, time_to_finish as time_taken,time_of_exec as Date from catissue_query_audit_logs logs join catissue_user usr on logs.run_by=usr.identifier order by time_to_finish desc limit 5;'
    top_5_most_run_saved_queries = 'select logs.query_id,concat(first_name,\' \',last_name) as name, count(*) as cnt, Max(time_of_exec) as Date, max(time_to_finish) as time_taken  from catissue_query_audit_logs logs join catissue_user user on logs.run_by = user.identifier where query_id is not null group by name,logs.query_id order by cnt desc limit 5;'
    top_5_users_running_most_queries = 'select concat(first_name,\' \',last_name) as name, count(*) as cnt, Max(time_of_exec) as Date, max(time_to_finish) as time_taken from catissue_query_audit_logs logs join catissue_user user on user.identifier = logs.run_by group by run_by limit 5;'
    
    print('============Top 5 slow running queries================')
    slow_queries_df=create_connection(top_5_slowest_running_queries)
    top_5_slowest_running_queries = add_url_to_dataframe(slow_queries_df)
    slowest_running_queries_html_table = top_5_slowest_running_queries.to_html(index=False)
    with open('slowest_running_queries.html', 'w') as f:
        f.write(slowest_running_queries_html_table)

    
    print('===========Top 5 most run saved queries=================')
    most_run_queries_df = create_connection(top_5_most_run_saved_queries)
    top_5_most_run_saved_queries =  add_url_to_dataframe(most_run_queries_df)
    top_5_most_run_saved_queries = top_5_most_run_saved_queries.drop('cnt',axis=1)
    # print(top_5_most_run_saved_queries.columns)
    most_run_saved_queries_html_table = top_5_most_run_saved_queries.to_html(index=False)
    with open('most_run_saved_queries.html',mode='w') as f:
        f.write(most_run_saved_queries_html_table)


    print('===========Top 5 users running most queries=================')
    users_running_most_queries=create_connection(top_5_users_running_most_queries)
    users_running_most_queries=users_running_most_queries.drop('cnt',axis=1)
    users_running_most_queries_html_table  = users_running_most_queries.to_html(index=False)
    with open('users_running_most_queries.html', mode='w') as f:
        f.write(users_running_most_queries_html_table)

    
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
    send_mail(combined_html)


    # add_url_to_dataframe(most_run_queries_df)
    # add_url_to_dataframe(slow_queries_df)

if __name__ == '__main__':
    main()
