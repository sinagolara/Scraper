
import yagmail
import os, sys
import datetime
from cleaning.time_management import eastern_time
from cleaning.file_management import read_zipped_csv
import pandas as pd

def send_test_email(to="sina.golara@gmail.com", body="Code is complete.", attachment='',subject="Code complete alert"):
    yag = yagmail.SMTP(user="codi.coder.2020@gmail.com", password='Burghal2020')
    contents = {'body': body}
    if os.path.isfile(attachment):
        contents['attachment':attachment]

    yag.send(
        to=to,
        subject=subject,
        contents=contents,
        )
    print('email sent')

def email_with_report(Date, subject=''):
    body = ''
    if subject == '':
        subject = f'Code complete at {Date}'
    else:
        subject += f' - {Date}'
    # add report if it exists
    report_path = f'./listings/report_listings_{Date}.csv'
    if os.path.exists(report_path):
        r = pd.read_csv(report_path)
        body = pd.DataFrame(r.iloc[0]).to_html()

    else:
        print(f'report {report_path} could not be found')

    # see if the report is zipped
    report_path = f'./listings/report_listings_{Date}.zip'
    if os.path.exists(report_path):
        print(f'loading {report_path} for email')
        r = read_zipped_csv(zip_path=report_path)[0]
        body += '\n' + pd.DataFrame(r.iloc[0]).to_html()

    body = body.replace('\n', ' ')

    if body != "":
        print(f'body {pd.read_html(body)}')
    send_test_email(subject=subject, body=body)



if __name__ == '__main__':
    Date_Time = eastern_time(fmt='%Y-%m-%d-%H:%M')
    Date = eastern_time(fmt='%Y-%m-%d')
    body = ''

    # add report if it exists
    report_path = f'./listings/report_listings_{Date}.csv'
    if os.path.exists(report_path):
        r = pd.read_csv(report_path)
        body = pd.DataFrame(r.iloc[0]).to_html()

    # see if the report is zipped
    report_path = f'./listings/report_listings_{Date}.zip'
    if os.path.exists(report_path):
        r = read_zipped_csv(zip_path=report_path)[0]
        body = pd.DataFrame(r.iloc[0]).to_html()

    if len(sys.argv) > 1:
        body = sys.argv[1].replace('--mode=client','') + '\n' + body

    body = body.replace('\n', ' ')

    send_test_email(subject=f'Code complete at {Date}', body=body)
