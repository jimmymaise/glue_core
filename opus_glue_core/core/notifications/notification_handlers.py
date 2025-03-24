import os
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import boto3
import requests
from jinja2 import Environment, FileSystemLoader

from opus_glue_core.core.constant import Constant

etl_local_path = str(Path(__file__).resolve().parents[1])


class NotificationHandler:

    @staticmethod
    def email_notification(subject, email_to, email_from, content, attachments, ses_region='us-west-2'):
        ses = boto3.client('ses', ses_region)
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = ','.join(email_to)
        msg.attach(MIMEText(content, 'html'))

        for file_path in attachments:
            with open(file_path, 'rb') as file_data:
                attachment = MIMEApplication(file_data.read())
            attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
            msg.attach(attachment)
        print('Prepare to send email {} to {} with the content\n {}'.format(subject, email_to, content))
        return ses.send_raw_email(RawMessage={'Data': msg.as_string()}, Source=email_from, Destinations=email_to)

    @staticmethod
    def generate_html_from_etl_email_template(template_string, data):
        template_dir = os.path.join(etl_local_path, Constant.EMAIL_TEMPLATE_FOLDER)
        jinja_env = Environment(loader=FileSystemLoader(template_dir),
                                autoescape=True)
        template = jinja_env.from_string(template_string)
        return template.render(data)

    @staticmethod
    def api_push_notification(api_url, body_message):
        response = requests.post(api_url, json=body_message)
        return response
