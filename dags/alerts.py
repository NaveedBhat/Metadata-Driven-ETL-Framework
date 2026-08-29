"""
alerts.py
---------
Shared success/failure email helpers.

Airflow only emails you automatically on FAILURE (via email_on_failure).
There's no built-in "email me on success" -- so we implement both here,
as callbacks, and attach them to every DAG. This way both dags behave
identically and you only maintain the email logic in one place.

CHANGE LOG (professional hardening pass)
    - task_failure_alert now includes the FULL traceback (via
      traceback.format_exception), not just str(exception). str() on an
      exception often only gives the final message line, which is rarely
      enough to debug a failure from the email alone.
    - All dynamic values (dag_id, task_id, exception text, log_url, etc.)
      are now HTML-escaped before being embedded in the email body. Error
      messages can legitimately contain '<', '>', or '&' (e.g. Python type
      errors, XML/HTML parsing errors) which would otherwise render as
      broken/truncated HTML in the alert email.
    - _format_context_line was previously defined but never called
      anywhere (dead code). It's now actually used to safely pull
      logical_date / execution_date in both callbacks.
"""

import html
import logging
import traceback

from airflow.utils.email import send_email  # type: ignore[import]
from config import ALERT_EMAIL_TO  # type: ignore[import]

logger = logging.getLogger("airflow.task")


def _format_context_line(context, key, default="N/A"):
    """
    Small helper so every callback pulls context values the same safe way --
    context.get() can legitimately return None (e.g. no exception object,
    or a missing logical_date on very old Airflow versions), so this
    normalizes that to a readable default instead of printing "None".
    """
    value = context.get(key, default)
    return value if value is not None else default


def _safe(value):
    """
    Escapes a value for safe embedding in the HTML email body. Exception
    messages, task ids, and dag ids can technically contain characters like
    '<' or '&', which would otherwise break the rendered email.
    """
    return html.escape(str(value))


def task_failure_alert(context):
    """
    on_failure_callback -- fires for ANY task that fails, in either dag.
    Attached at the DAG level via default_args so every task is covered
    automatically, including ones added later.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = _format_context_line(context, "logical_date", context.get("execution_date", "N/A"))
    try_number = context["task_instance"].try_number
    log_url = context["task_instance"].log_url

    exception = context.get("exception")
    if exception is not None:
        # Full traceback is far more useful for debugging than str(exception)
        # alone, which often only has the final error message.
        # format_exception() works directly on the stored __traceback__ so it
        # is correct here in a callback context (unlike format_exc(), which
        # only captures the active exception inside an except block).
        error_detail = "".join(
            traceback.format_exception(type(exception), exception, exception.__traceback__)
        )
    else:
        error_detail = "N/A"

    subject = f"[FAILED] Airflow DAG '{dag_id}' - task '{task_id}'"
    html_content = f"""
    <h3>A task has failed in the customer data pipeline</h3>
    <table border="1" cellpadding="6" cellspacing="0">
        <tr><td><b>DAG</b></td><td>{_safe(dag_id)}</td></tr>
        <tr><td><b>Failed Task</b></td><td>{_safe(task_id)}</td></tr>
        <tr><td><b>Run (logical date)</b></td><td>{_safe(execution_date)}</td></tr>
        <tr><td><b>Attempt</b></td><td>{_safe(try_number)}</td></tr>
        <tr><td><b>Error</b></td><td><pre style="white-space:pre-wrap;word-break:break-word;">{_safe(error_detail)}</pre></td></tr>
    </table>
    <p>Full task logs: <a href="{_safe(log_url)}">{_safe(log_url)}</a></p>
    """
    logger.error("Sending failure alert email for %s.%s", dag_id, task_id)
    send_email(to=ALERT_EMAIL_TO, subject=subject, html_content=html_content)


def dag_success_alert(context, summary_lines=None):
    """
    Called explicitly from the LAST task of each dag (via on_success_callback
    on that task) once the whole pipeline has actually finished successfully.
    summary_lines: optional list[str] of extra detail (row counts etc.)
    pulled from XCom by the caller, shown in the email body.
    """
    dag_id = context["dag"].dag_id
    execution_date = _format_context_line(context, "logical_date", context.get("execution_date", "N/A"))

    subject = f"[SUCCESS] Airflow DAG '{dag_id}' completed"
    extra_html = ""
    if summary_lines:
        items = "".join(f"<li>{_safe(line)}</li>" for line in summary_lines)
        extra_html = f"<ul>{items}</ul>"

    html_content = f"""
    <h3>DAG '{_safe(dag_id)}' completed successfully</h3>
    <p><b>Run (logical date):</b> {_safe(execution_date)}</p>
    {extra_html}
    """
    logger.info("Sending success alert email for %s", dag_id)
    send_email(to=ALERT_EMAIL_TO, subject=subject, html_content=html_content)
