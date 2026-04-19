import os
import yaml
import html
import dagster as dg
from pathlib import Path
from exchangelib import Credentials, Account, Configuration, DELEGATE, Message, HTMLBody


class DagsterEmailNotificationService:
    """
    Service for sending HTML email notifications for Dagster run status via Exchange.
    
    This class handles:
    - Exchange/SMTP connection management
    - HTML email template generation
    - Email recipient lookup from YAML configuration
    - Run status notification formatting
    """
    
    def __init__(
        self,
        exchange_username: str,
        exchange_password: str,
        exchange_server: str,
        exchange_email: str,
        email_recipients_file: Path,
        dagster_ui_url: str = "http://localhost:3001"
    ):
        self.exchange_username = exchange_username
        self.exchange_password = exchange_password
        self.exchange_server = exchange_server
        self.exchange_email = exchange_email
        self.email_recipients_file = email_recipients_file
        self.dagster_ui_url = dagster_ui_url
        
        # Initialize Exchange connection
        self.credentials = Credentials(
            username=self.exchange_username,
            password=self.exchange_password
        )
        
        self.config = Configuration(
            server=self.exchange_server,
            credentials=self.credentials
        )
        
        self.account = Account(
            primary_smtp_address=self.exchange_email,
            credentials=self.credentials,
            config=self.config,
            autodiscover=False,
            access_type=DELEGATE,
        )
    
    def send_email(self, subject: str, html_content: str, to_recipients: list[str]):
        """
        Sends an HTML email notification via Exchange.
        
        Args:
            subject: Email subject line.
            html_content: HTML body content.
            to_recipients: List of recipient email addresses.
        """
        message = Message(
            account=self.account,
            subject=subject,
            body=HTMLBody(html_content),
            to_recipients=to_recipients,
        )
        message.send()
    
    def get_email_recipients(self, job_name: str) -> list[str]:
        """
        Load email recipients for a specific job from YAML configuration.
        
        Args:
            job_name: Name of the Dagster job.
        
        Returns:
            list[str]: Unique list of email addresses (job owners + admins).
        """
        with open(self.email_recipients_file) as f:
            data = yaml.safe_load(f)
        
        job_owners = data.get("jobs", {}).get(job_name, [])
        admins = data.get("admins", [])

        return list(set(job_owners + admins))
    
    def generate_email_html(self, title: str, run_info: str, color: str, details_html: str = "") -> str:
        """
        Generate a clean HTML template for email notifications.
        
        Args:
            title: Email title/header.
            run_info: HTML table rows with run information.
            color: 'red' or 'green' for header background color.
            details_html: Additional HTML content for error details or other info.
        
        Returns:
            str: Complete HTML email template.
        """
        header_bg = "#d9534f" if color == "red" else "#5cb85c"
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <div style="max-width: 600px; margin: 0 auto; border: 1px solid #e0e0e0; border-radius: 5px; overflow: hidden;">
                <div style="background-color: {header_bg}; color: white; padding: 15px; text-align: center;">
                    <h2 style="margin: 0;">{title}</h2>
                </div>
                
                <div style="padding: 20px;">
                    <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                        {run_info}
                    </table>
                    
                    {details_html}
                    
                    <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
                    <p style="font-size: 12px; color: #999; text-align: center;">
                        Sent by Dagster Pipeline Monitor
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
    
    def get_run_info_rows(self, job_name: str, run_id: str) -> str:
        """
        Generate HTML table rows for run information.
        
        Args:
            job_name: Name of the Dagster job.
            run_id: Dagster run ID.
        
        Returns:
            str: HTML table rows with job name and run ID.
        """
        run_url = f"{self.dagster_ui_url}/runs/{run_id}"
        return f"""
        <tr>
            <td style="padding: 8px; font-weight: bold; width: 120px;">Job Name:</td>
            <td style="padding: 8px;">{job_name}</td>
        </tr>
        <tr>
            <td style="padding: 8px; font-weight: bold;">Run ID:</td>
            <td style="padding: 8px;">
                <a href="{run_url}" style="color: #007bff; text-decoration: none;">{run_id}</a>
            </td>
        </tr>
        """
    
    def send_success_notification(self, job_name: str, run_id: str):
        """
        Send a success notification email for a completed Dagster run.
        
        Args:
            job_name: Name of the Dagster job.
            run_id: Dagster run ID.
        """
        run_info = self.get_run_info_rows(job_name, run_id)
        run_info += """
        <tr>
            <td style="padding: 8px; font-weight: bold;">Status:</td>
            <td style="padding: 8px; color: green; font-weight: bold;">SUCCESS ✅</td>
        </tr>
        """

        message_html = self.generate_email_html(
            title="Pipeline Succeeded",
            run_info=run_info,
            color="green",
            details_html="<p>The pipeline run completed successfully. No actions are required.</p>"
        )
        
        self.send_email(
            subject=f'✅ Success: {job_name}',
            html_content=message_html,
            to_recipients=self.get_email_recipients(job_name)
        )
    
    def send_failure_notification(self, job_name: str, run_id: str, failure_context: dg.RunFailureSensorContext):
        """
        Send a failure notification email for a failed Dagster run.
        
        Args:
            job_name: Name of the Dagster job.
            run_id: Dagster run ID.
            failure_context: Dagster failure sensor context with error details.
        """
        run_info = self.get_run_info_rows(job_name, run_id)
        run_info += """
        <tr>
            <td style="padding: 8px; font-weight: bold;">Status:</td>
            <td style="padding: 8px; color: red; font-weight: bold;">FAILED ❌</td>
        </tr>
        """

        # Process specific step errors
        error_details = ""
        step_failures = failure_context.get_step_failure_events()
        
        if step_failures:
            for event in step_failures:
                step_key = event.step_key
                error_log = html.escape(event.event_specific_data.error.to_string())
                
                error_details += f"""
                <div style="margin-top: 20px;">
                    <h3 style="margin-bottom: 5px; color: #d9534f;">Failed Step: {step_key}</h3>
                    <div style="background: #f8f9fa; border: 1px solid #ddd; padding: 10px; border-radius: 4px; overflow-x: auto;">
                        <pre style="margin: 0; font-size: 11px; font-family: Consolas, monospace; white-space: pre-wrap;">{error_log}</pre>
                    </div>
                </div>
                """
        else:
            general_error = html.escape(failure_context.failure_event.message or "Unknown error")
            error_details = f"<p><strong>Error Message:</strong> {general_error}</p>"

        message_html = self.generate_email_html(
            title="Pipeline Failed",
            run_info=run_info,
            color="red",
            details_html=error_details
        )

        self.send_email(
            subject=f'❌ Failed: {job_name}',
            html_content=message_html,
            to_recipients=self.get_email_recipients(job_name)
        )

