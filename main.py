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
        database = database
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

# def convert_to_html(query):
#         html_table = create_connection(query).to_html(index=False)
#         file_name = f""
#         with open('slowest_running_queries.html', 'w') as f:
#             f.write(html_table)


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
            smtp.login('sahilratnaparkhi1@gmail.com', 'sftu okmq ryop bdpd')
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



def main():
    top_5_slowest_running_queries = 'select concat(first_name,\' \',last_name) as user, time_to_finish as time_taken,time_of_exec as Date from catissue_query_audit_logs logs join catissue_user usr on logs.run_by=usr.identifier order by time_to_finish desc limit 5;'
    top_5_most_run_saved_queries = 'select concat(first_name,\' \',last_name) as name,run_by, count(*) as cnt, Max(time_of_exec) as Date, max(time_to_finish) as time_taken  from catissue_query_audit_logs logs join catissue_user user on logs.run_by = user.identifier where query_id is not null group by name,run_by order by cnt desc limit 5;'
    top_5_users_running_most_queries = 'select concat(first_name,\' \',last_name) as name, run_by, count(*) as cnt, Max(time_of_exec) as Date, max(time_to_finish) as time_taken from catissue_query_audit_logs logs join catissue_user user on user.identifier = logs.run_by group by run_by limit 5;'
    
    print('============Top 5 slow running queries================')
    slowest_running_queries_html_table = create_connection(top_5_slowest_running_queries).to_html(index=False)
    with open('slowest_running_queries.html', 'w') as f:
        f.write(slowest_running_queries_html_table)

    
    print('===========Top 5 most run saved queries=================')
    most_run_saved_queries_html_table = create_connection(top_5_most_run_saved_queries).to_html(index=False)
    with open('most_run_saved_queries.html',mode='w') as f:
        f.write(most_run_saved_queries_html_table)


    print('===========Top 5 users running most queries=================')
    users_running_most_queries_html_table  = create_connection(top_5_users_running_most_queries).to_html(index=False)
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
        <p>Regards,<br>Your Automation Script</p>
    </body>
    </html>
    """
    send_mail(combined_html)

if __name__ == '__main__':
    main()