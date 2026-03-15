from __future__ import annotations

import json
import hashlib
import hmac
import logging
import os
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlparse
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import docker
import requests
from requests.auth import HTTPBasicAuth


LOG = logging.getLogger("telegram_alert_bot")

STATE_EMOJI = {
    "failed": "❌",
    "success": "✅",
    "running": "🟡",
    "healthy": "✅",
    "unhealthy": "❌",
    "missing": "❌",
}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        LOG.warning("Invalid integer for %s=%r, using %s", name, raw, default)
        return default


def env_csv(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


def env_value(name: str, default: str = "") -> str:
    file_path = os.getenv(f"{name}_FILE", "").strip()
    if file_path:
        try:
            return Path(file_path).read_text().strip()
        except Exception as exc:
            LOG.warning("Failed to read secret file for %s at %s: %s", name, file_path, exc)
    return os.getenv(name, default)


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = {
            "dag_failures": {},
            "dag_status": {},
            "services": {},
            "daily_reports": {},
        }
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            self.data = json.loads(self.path.read_text())
        except Exception as exc:
            LOG.warning("Failed to load state file %s: %s", self.path, exc)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True))


class TelegramAlerter:
    def __init__(self, token: str, chat_id: str, timeout: int = 15) -> None:
        self.token = token
        self.chat_id = chat_id
        self.timeout = timeout

    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def send(self, text: str) -> None:
        if not self.enabled():
            LOG.warning("Telegram is not configured. Skipping alert: %s", text.splitlines()[0])
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        resp = requests.post(
            url,
            timeout=self.timeout,
            json={"chat_id": self.chat_id, "text": text[:4000]},
        )
        resp.raise_for_status()


class AirflowMonitor:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        project_root: Path,
        max_dags: int,
        max_log_lines: int,
        request_timeout: int,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(username, password)
        self.project_root = project_root
        self.max_dags = max_dags
        self.max_log_lines = max_log_lines
        self.timeout = request_timeout
        self.session = requests.Session()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, auth=self.auth, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def list_failed_runs(self) -> list[dict[str, Any]]:
        dags_payload = self._get("/api/v1/dags", params={"limit": self.max_dags, "only_active": "true"})
        failed_runs: list[dict[str, Any]] = []
        for dag in dags_payload.get("dags", []):
            dag_id = dag["dag_id"]
            runs_payload = self._get(
                f"/api/v1/dags/{quote(dag_id, safe='')}/dagRuns",
                params={"limit": 5, "order_by": "-start_date"},
            )
            for run in runs_payload.get("dag_runs", []):
                if run.get("state") == "failed":
                    failed_runs.append({"dag_id": dag_id, "run": run})
        return failed_runs

    def list_latest_runs(self) -> list[dict[str, Any]]:
        dags_payload = self._get("/api/v1/dags", params={"limit": self.max_dags, "only_active": "true"})
        latest_runs: list[dict[str, Any]] = []
        for dag in dags_payload.get("dags", []):
            dag_id = dag["dag_id"]
            runs_payload = self._get(
                f"/api/v1/dags/{quote(dag_id, safe='')}/dagRuns",
                params={"limit": 1, "order_by": "-start_date"},
            )
            runs = runs_payload.get("dag_runs", [])
            if runs:
                latest_runs.append({"dag_id": dag_id, "run": runs[0]})
        return latest_runs

    def build_failure_message(self, dag_id: str, run: dict[str, Any]) -> str:
        run_id = run["dag_run_id"]
        payload = self._get(
            f"/api/v1/dags/{quote(dag_id, safe='')}/dagRuns/{quote(run_id, safe='')}/taskInstances"
        )
        failed_tasks = [task for task in payload.get("task_instances", []) if task.get("state") == "failed"]
        lines = [
            "❌ AIRFLOW DAG FAILED",
            f"dag_id: {dag_id}",
            f"run_id: {run_id}",
            f"state: {emoji_for(run.get('state'))} {run.get('state')}",
            f"start_date: {run.get('start_date')}",
            f"end_date: {run.get('end_date')}",
        ]
        if failed_tasks:
            lines.append("failed_tasks:")
            for task in failed_tasks[:3]:
                lines.append(
                    f"- {task.get('task_id')} state={emoji_for(task.get('state'))} {task.get('state')} try_number={task.get('try_number')}"
                )
        return "\n".join(lines)

    def _read_failed_task_log(self, dag_id: str, run_id: str, task_id: str) -> list[str]:
        task_dir = (
            self.project_root
            / "airflow"
            / "logs"
            / f"dag_id={dag_id}"
            / f"run_id={run_id}"
            / f"task_id={task_id}"
        )
        if not task_dir.exists():
            return []
        attempt_logs = sorted(task_dir.glob("attempt=*.log"))
        if not attempt_logs:
            return []
        try:
            content = attempt_logs[-1].read_text(errors="replace").splitlines()
        except Exception as exc:
            return [f"<failed to read log: {exc}>"]
        tail = content[-self.max_log_lines :]
        return tail


class DockerMonitor:
    def __init__(self, project_name: str, monitored_services: list[str], max_log_lines: int) -> None:
        self.project_name = project_name
        self.monitored_services = set(monitored_services)
        self.max_log_lines = max_log_lines
        self.client = docker.from_env()

    def collect_failures(self) -> list[dict[str, str]]:
        failures: list[dict[str, str]] = []
        service_map = self._containers_by_service()
        for service_name in sorted(self.monitored_services):
            container = service_map.get(service_name)
            if not container:
                failures.append(
                    {
                        "service": service_name,
                        "fingerprint": f"{service_name}:missing",
                        "message": self._format_missing(service_name),
                    }
                )
                continue
            container.reload()
            state = container.attrs.get("State", {})
            status = state.get("Status", "unknown")
            health = state.get("Health", {}).get("Status")
            failing = status != "running" or (health and health != "healthy")
            if not failing:
                continue
            fingerprint = f"{service_name}:{status}:{health}:{state.get('FinishedAt')}:{state.get('ExitCode')}"
            failures.append(
                {
                    "service": service_name,
                    "fingerprint": fingerprint,
                    "message": self._format_container_failure(service_name, container, state, health),
                }
            )
        return failures

    def _containers_by_service(self) -> dict[str, docker.models.containers.Container]:
        containers = self.client.containers.list(all=True, filters={"label": f"com.docker.compose.project={self.project_name}"})
        result: dict[str, docker.models.containers.Container] = {}
        for container in containers:
            service_name = container.labels.get("com.docker.compose.service")
            if service_name:
                result[service_name] = container
        return result

    def _format_missing(self, service_name: str) -> str:
        return "\n".join(
            [
                "❌ CLUSTER SERVICE FAILED",
                f"service: {service_name}",
                "status: ❌ missing",
                "details: container not found in the compose project",
            ]
        )

    def _format_container_failure(
        self,
        service_name: str,
        container: docker.models.containers.Container,
        state: dict[str, Any],
        health: str | None,
    ) -> str:
        lines = [
            "❌ CLUSTER SERVICE FAILED",
            f"service: {service_name}",
            f"container: {container.name}",
            f"status: {emoji_for(state.get('Status'))} {state.get('Status')}",
            f"health: {emoji_for(health, 'ℹ️')} {health or 'n/a'}",
            f"exit_code: {state.get('ExitCode')}",
            f"oom_killed: {state.get('OOMKilled')}",
            f"error: {state.get('Error') or '-'}",
            f"started_at: {state.get('StartedAt')}",
            f"finished_at: {state.get('FinishedAt')}",
        ]
        return "\n".join(lines)

    def collect_service_states(self) -> list[dict[str, Any]]:
        service_map = self._containers_by_service()
        states: list[dict[str, Any]] = []
        for service_name in sorted(self.monitored_services):
            container = service_map.get(service_name)
            if not container:
                states.append(
                    {
                        "service": service_name,
                        "status": "missing",
                        "health": None,
                        "container": None,
                    }
                )
                continue
            container.reload()
            state = container.attrs.get("State", {})
            states.append(
                {
                    "service": service_name,
                    "status": state.get("Status", "unknown"),
                    "health": state.get("Health", {}).get("Status"),
                    "container": container,
                }
            )
        return states

    def collect_memory_usage(self, services: list[str] | None = None) -> list[dict[str, Any]]:
        service_filter = set(services or self.monitored_services)
        service_map = self._containers_by_service()
        result: list[dict[str, Any]] = []
        for service_name in sorted(service_filter):
            container = service_map.get(service_name)
            if not container:
                result.append({"service": service_name, "error": "container not found"})
                continue
            try:
                stats = container.stats(stream=False)
            except Exception as exc:
                result.append({"service": service_name, "error": str(exc)})
                continue
            memory_stats = stats.get("memory_stats", {})
            usage = memory_stats.get("usage")
            stats_values = memory_stats.get("stats", {})
            cache = (
                stats_values.get("inactive_file")
                or stats_values.get("total_inactive_file")
                or stats_values.get("cache")
                or 0
            )
            working_set = usage - cache if usage is not None else None
            if working_set is not None and working_set < 0:
                working_set = 0
            limit = memory_stats.get("limit")
            result.append(
                {
                    "service": service_name,
                    "usage_bytes": working_set,
                    "limit_bytes": limit,
                }
            )
        return result


class TrinoReporter:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        catalog: str,
        schemas: list[str],
        http_scheme: str,
        verify: bool,
        request_timeout: int,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.catalog = catalog
        self.schemas = schemas
        self.http_scheme = http_scheme
        self.verify = verify
        self.timeout = request_timeout
        self.session = requests.Session()

    def _query(self, sql: str) -> list[list[Any]]:
        base_url = f"{self.http_scheme}://{self.host}:{self.port}"
        headers = {
            "X-Trino-User": self.user,
            "X-Trino-Catalog": self.catalog,
        }
        resp = self.session.post(
            f"{base_url}/v1/statement",
            data=sql,
            headers=headers,
            auth=HTTPBasicAuth(self.user, self.password),
            timeout=self.timeout,
            verify=self.verify,
        )
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("data", [])
        next_uri = payload.get("nextUri")
        error = payload.get("error")

        while next_uri and not error:
            resp = self.session.get(
                next_uri,
                auth=HTTPBasicAuth(self.user, self.password),
                timeout=self.timeout,
                verify=self.verify,
            )
            resp.raise_for_status()
            payload = resp.json()
            rows.extend(payload.get("data", []))
            next_uri = payload.get("nextUri")
            error = payload.get("error")

        if error:
            message = error.get("message", "unknown Trino error")
            raise RuntimeError(message)
        return rows

    def collect_tables(self) -> list[dict[str, str]]:
        if not self.schemas:
            return []
        quoted_schemas = []
        for schema in self.schemas:
            quoted_schemas.append("'" + schema.replace("'", "''") + "'")
        schema_list = ",".join(quoted_schemas)
        query = f"""
        SELECT table_schema, table_name
        FROM {self.catalog}.information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema IN ({schema_list})
        ORDER BY table_schema, table_name
        """
        return [{"schema": table_schema, "table": table_name} for table_schema, table_name in self._query(query)]


class MinioReporter:
    def __init__(
        self,
        docker_monitor: DockerMonitor,
        buckets: list[str],
        endpoint: str,
        access_key: str,
        secret_key: str,
        request_timeout: int,
        minio_service_name: str = "minio",
    ) -> None:
        self.docker_monitor = docker_monitor
        self.buckets = buckets
        self.endpoint = endpoint.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self.timeout = request_timeout
        self.minio_service_name = minio_service_name
        self.session = requests.Session()

    def collect_bucket_sizes(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for bucket in self.buckets:
            total_bytes = 0
            object_count = 0
            continuation_token: str | None = None
            while True:
                payload = self._list_objects_v2(bucket, continuation_token)
                namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
                for item in payload.findall("s3:Contents", namespace):
                    size_text = item.findtext("s3:Size", default="0", namespaces=namespace)
                    total_bytes += int(size_text)
                    object_count += 1
                is_truncated = payload.findtext("s3:IsTruncated", default="false", namespaces=namespace) == "true"
                if not is_truncated:
                    break
                continuation_token = payload.findtext(
                    "s3:NextContinuationToken",
                    default="",
                    namespaces=namespace,
                ) or None
            result.append(
                {
                    "bucket": bucket,
                    "bytes": total_bytes,
                    "objects": object_count,
                }
            )
        return result

    def _list_objects_v2(self, bucket: str, continuation_token: str | None) -> ElementTree.Element:
        params = {"list-type": "2"}
        if continuation_token:
            params["continuation-token"] = continuation_token
        resp = self._signed_request("GET", f"/{bucket}", params=params)
        resp.raise_for_status()
        return ElementTree.fromstring(resp.text)

    def _signed_request(self, method: str, canonical_uri: str, params: dict[str, str]) -> requests.Response:
        parsed = urlparse(self.endpoint)
        host = parsed.netloc
        canonical_query = urlencode(sorted(params.items()))
        now = datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        canonical_headers = f"host:{host}\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:{amz_date}\n"
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                "UNSIGNED-PAYLOAD",
            ]
        )
        credential_scope = f"{date_stamp}/us-east-1/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signing_key = self._signature_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        headers = {
            "x-amz-date": amz_date,
            "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
            "Authorization": authorization,
        }
        url = f"{self.endpoint}{canonical_uri}?{canonical_query}"
        return self.session.request(method, url, headers=headers, timeout=self.timeout)

    def _signature_key(self, date_stamp: str) -> bytes:
        key_date = hmac.new(("AWS4" + self.secret_key).encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
        key_region = hmac.new(key_date, b"us-east-1", hashlib.sha256).digest()
        key_service = hmac.new(key_region, b"s3", hashlib.sha256).digest()
        return hmac.new(key_service, b"aws4_request", hashlib.sha256).digest()

    def path_size_bytes(self, path: str) -> int:
        service_map = self.docker_monitor._containers_by_service()
        container = service_map.get(self.minio_service_name)
        if not container:
            raise RuntimeError("MinIO container not found")
        size_exec = container.exec_run(["du", "-sb", path])
        if size_exec.exit_code != 0:
            output = size_exec.output.decode("utf-8", errors="replace").strip()
            if "No such file or directory" in output:
                return 0
            raise RuntimeError(output or f"du failed for {path}")
        return int(size_exec.output.decode("utf-8", errors="replace").split()[0])


def trino_storage_paths() -> list[dict[str, str]]:
    return [
        {"name": "raw", "path": "/data/dwh/warehouse/raw"},
        {"name": "stg", "path": "/data/dwh/warehouse/stg"},
        {"name": "ods", "path": "/data/dwh/warehouse/ods.db"},
        {"name": "dma", "path": "/data/dwh/warehouse/dma.db"},
    ]


class DailyReportScheduler:
    def __init__(self, time_of_day: str, timezone_name: str) -> None:
        hour, minute = self._parse_time(time_of_day)
        self.hour = hour
        self.minute = minute
        self.timezone = ZoneInfo(timezone_name)

    def _parse_time(self, time_of_day: str) -> tuple[int, int]:
        try:
            hour_str, minute_str = time_of_day.split(":", 1)
            hour = int(hour_str)
            minute = int(minute_str)
        except Exception as exc:
            raise ValueError(f"Invalid ALERT_BOT_DAILY_REPORT_TIME={time_of_day!r}") from exc
        if hour not in range(24) or minute not in range(60):
            raise ValueError(f"Invalid ALERT_BOT_DAILY_REPORT_TIME={time_of_day!r}")
        return hour, minute

    def should_send(self, state: StateStore) -> bool:
        now = datetime.now(self.timezone)
        scheduled = now.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0)
        if now < scheduled:
            return False
        report_key = f"{self.timezone.key}:{self.hour:02d}:{self.minute:02d}"
        return state.data.setdefault("daily_reports", {}).get(report_key) != now.date().isoformat()

    def mark_sent(self, state: StateStore) -> None:
        now = datetime.now(self.timezone)
        report_key = f"{self.timezone.key}:{self.hour:02d}:{self.minute:02d}"
        state.data.setdefault("daily_reports", {})[report_key] = now.date().isoformat()


def format_bytes(value: int | None) -> str:
    if value is None:
        return "n/a"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} {unit}"
        size /= 1024
    return f"{int(value)} B"


def format_ratio(usage: int | None, limit: int | None) -> str:
    if usage is None:
        return "n/a"
    if not limit:
        return format_bytes(usage)
    return f"{format_bytes(usage)} / {format_bytes(limit)}"


def memory_status(usage: int | None, limit: int | None) -> str:
    if usage is None or not limit:
        return "ℹ️"
    ratio = usage / limit
    if ratio < 0.7:
        return "✅"
    if ratio <= 0.9:
        return "⚠️"
    return "❌"


def format_percent(usage: int | None, limit: int | None) -> str:
    if usage is None or not limit:
        return "n/a"
    return f"{usage / limit * 100:.1f}%"


def build_daily_report(
    docker_monitor: DockerMonitor,
    trino_reporter: TrinoReporter,
    minio_reporter: MinioReporter,
    timezone_name: str,
) -> str:
    lines = [
        "📊 DAILY CLUSTER REPORT",
        f"timezone: {timezone_name}",
    ]

    lines.append("")
    lines.append("cluster_state:")
    for item in docker_monitor.collect_service_states():
        health = item.get("health")
        suffix = f", health={emoji_for(health, '-')} {health}" if health else ""
        lines.append(f"- {item['service']}: {emoji_for(item['status'])} {item['status']}{suffix}")

    lines.append("")
    lines.append("ram_usage:")
    total_usage_bytes = 0
    total_limit_bytes = 0
    seen_limit = False
    for item in docker_monitor.collect_memory_usage():
        if item.get("error"):
            lines.append(f"- {item['service']}: {item['error']}")
            continue
        usage = item.get("usage_bytes")
        limit = item.get("limit_bytes")
        if usage is not None:
            total_usage_bytes += usage
        if limit:
            total_limit_bytes += limit
            seen_limit = True
        lines.append(
            f"- {item['service']}: {memory_status(usage, limit)} {format_ratio(usage, limit)} ({format_percent(usage, limit)})"
        )
    lines.append(
        f"- total usage memory: {memory_status(total_usage_bytes, total_limit_bytes if seen_limit else None)} "
        f"{format_ratio(total_usage_bytes, total_limit_bytes if seen_limit else None)} "
        f"({format_percent(total_usage_bytes, total_limit_bytes if seen_limit else None)})"
    )

    lines.append("")
    lines.append("trino_table_storage:")
    try:
        storage_groups = []
        for item in trino_storage_paths():
            storage_groups.append(
                {
                    "name": item["name"],
                    "bytes": minio_reporter.path_size_bytes(item["path"]),
                }
            )
        total_bytes = sum(item["bytes"] for item in storage_groups)
        lines.append(f"- total: {format_bytes(total_bytes)}")
        for item in storage_groups:
            lines.append(f"- {item['name']}: {format_bytes(item['bytes'])}")
    except Exception as exc:
        lines.append(f"- failed to collect Trino table sizes: {exc}")

    lines.append("")
    lines.append("minio_storage:")
    try:
        bucket_sizes = minio_reporter.collect_bucket_sizes()
        for item in bucket_sizes:
            lines.append(f"- {item['bucket']}: {format_bytes(item['bytes'])}, objects={item['objects']}")
    except Exception as exc:
        lines.append(f"- failed to collect MinIO usage: {exc}")

    report = "\n".join(lines)
    if len(report) <= 4000:
        return report
    return report[:3950] + "\n... report truncated"


def configure_logging() -> None:
    level = os.getenv("ALERT_BOT_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


def format_exception_message(title: str, details: str) -> str:
    return "\n".join([title, f"details: {details}"])


def format_recovery_message(title: str, details: list[str]) -> str:
    return "\n".join([title, *details])


def emoji_for(value: str | None, default: str = "ℹ️") -> str:
    if not value:
        return default
    return STATE_EMOJI.get(value.lower(), default)


def main() -> int:
    configure_logging()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    poll_interval = env_int("ALERT_BOT_POLL_INTERVAL_SECONDS", 30)
    request_timeout = env_int("ALERT_BOT_REQUEST_TIMEOUT_SECONDS", 15)
    max_log_lines = env_int("ALERT_BOT_MAX_LOG_LINES", 40)
    max_dags = env_int("ALERT_BOT_MAX_DAGS", 100)
    project_name = os.getenv("COMPOSE_PROJECT_NAME", "wather")
    daily_report_time = os.getenv("ALERT_BOT_DAILY_REPORT_TIME", "11:00")
    daily_report_timezone = os.getenv("ALERT_BOT_DAILY_REPORT_TIMEZONE", "Europe/Moscow")
    monitored_services = env_csv(
        "ALERT_BOT_MONITORED_SERVICES",
        ["airflow-db", "airflow-scheduler", "airflow-web", "hive-metastore", "metastore-db", "minio", "trino"],
    )
    state = StateStore(Path(os.getenv("ALERT_BOT_STATE_FILE", "/state/alert_state.json")))
    alerter = TelegramAlerter(token=token, chat_id=chat_id, timeout=request_timeout)
    airflow = AirflowMonitor(
        base_url=os.getenv("AIRFLOW_BASE_URL", "http://airflow-web:8080"),
        username=os.getenv("AIRFLOW_USERNAME", "admin"),
        password=env_value("AIRFLOW_PASSWORD", "admin"),
        project_root=Path(os.getenv("ALERT_BOT_PROJECT_ROOT", "/opt/alert-bot/project")),
        max_dags=max_dags,
        max_log_lines=max_log_lines,
        request_timeout=request_timeout,
    )
    docker_monitor = DockerMonitor(project_name=project_name, monitored_services=monitored_services, max_log_lines=max_log_lines)
    daily_report_scheduler = DailyReportScheduler(daily_report_time, daily_report_timezone)
    trino_reporter = TrinoReporter(
        host=os.getenv("TRINO_HOST", "trino"),
        port=env_int("TRINO_PORT", 8443),
        user=os.getenv("TRINO_USER", "ilya"),
        password=env_value("TRINO_PASSWORD"),
        catalog=os.getenv("TRINO_CATALOG", "hive"),
        schemas=env_csv("TRINO_SCHEMAS", ["raw", "stg", "ods"]),
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "https"),
        verify=os.getenv("TRINO_VERIFY", "false").lower() == "true",
        request_timeout=request_timeout,
    )
    minio_reporter = MinioReporter(
        docker_monitor=docker_monitor,
        buckets=env_csv("MINIO_BUCKETS", ["raw", "dwh"]),
        endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        secret_key=env_value("MINIO_SECRET_KEY"),
        request_timeout=request_timeout,
    )

    LOG.info("Alert bot started on %s for compose project %s", hostname(), project_name)
    if not alerter.enabled():
        LOG.warning("Telegram credentials are not configured. Alerts will only be logged.")

    while True:
        dirty = False

        try:
            latest_runs = airflow.list_latest_runs()
            failed_runs = airflow.list_failed_runs()
            seen_services = state.data.setdefault("services", {})
            if "airflow-api" in seen_services:
                message = format_recovery_message(
                    "✅ AIRFLOW API RECOVERED",
                    ["details: Airflow API is reachable again"],
                )
                LOG.info(message)
                alerter.send(message)
                del seen_services["airflow-api"]
                dirty = True
            dag_status = state.data.setdefault("dag_status", {})
            for item in latest_runs:
                dag_id = item["dag_id"]
                run = item["run"]
                current_state = run.get("state")
                current_run_id = run.get("dag_run_id")
                previous = dag_status.get(dag_id)
                if (
                    current_state == "success"
                    and previous
                    and previous.get("state") == "failed"
                ):
                    message = format_recovery_message(
                        "✅ AIRFLOW DAG RECOVERED",
                        [
                            f"dag_id: {dag_id}",
                            f"failed_run_id: {previous.get('run_id')}",
                            f"recovered_run_id: {current_run_id}",
                            "state: ✅ success",
                            f"end_date: {run.get('end_date')}",
                        ],
                    )
                    LOG.info(message)
                    alerter.send(message)
                dag_status[dag_id] = {
                    "state": current_state,
                    "run_id": current_run_id,
                    "end_date": run.get("end_date"),
                }
                dirty = True

            seen_runs = state.data.setdefault("dag_failures", {})
            for item in failed_runs:
                dag_id = item["dag_id"]
                run = item["run"]
                key = f"{dag_id}:{run['dag_run_id']}"
                fingerprint = f"{run.get('state')}:{run.get('end_date')}"
                if seen_runs.get(key) == fingerprint:
                    continue
                message = airflow.build_failure_message(dag_id, run)
                LOG.error(message)
                alerter.send(message)
                seen_runs[key] = fingerprint
                dirty = True
        except Exception as exc:
            seen_services = state.data.setdefault("services", {})
            fingerprint = str(exc)
            if seen_services.get("airflow-api") != fingerprint:
                message = format_exception_message("❌ AIRFLOW API UNAVAILABLE", str(exc))
                LOG.error(message)
                alerter.send(message)
                seen_services["airflow-api"] = fingerprint
                dirty = True

        try:
            service_failures = docker_monitor.collect_failures()
            seen_services = state.data.setdefault("services", {})
            active_service_names = set()
            for failure in service_failures:
                service_name = failure["service"]
                active_service_names.add(service_name)
                fingerprint = failure["fingerprint"]
                if seen_services.get(service_name) == fingerprint:
                    continue
                LOG.error(failure["message"])
                alerter.send(failure["message"])
                seen_services[service_name] = fingerprint
                dirty = True
            for service_name in list(seen_services):
                if service_name in docker_monitor.monitored_services and service_name not in active_service_names:
                    message = format_recovery_message(
                        "✅ CLUSTER SERVICE RECOVERED",
                        [
                            f"service: {service_name}",
                            "status: 🟡 running",
                            "details: service health returned to normal",
                        ],
                    )
                    LOG.info(message)
                    alerter.send(message)
                    del seen_services[service_name]
                    dirty = True
        except Exception:
            LOG.exception("Docker monitoring iteration failed")

        try:
            if daily_report_scheduler.should_send(state):
                message = build_daily_report(
                    docker_monitor=docker_monitor,
                    trino_reporter=trino_reporter,
                    minio_reporter=minio_reporter,
                    timezone_name=daily_report_timezone,
                )
                LOG.info("Sending daily cluster report")
                alerter.send(message)
                daily_report_scheduler.mark_sent(state)
                dirty = True
        except Exception:
            LOG.exception("Daily report iteration failed")

        if dirty:
            state.save()
        time.sleep(poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
