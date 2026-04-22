import logging
import time


activity_logger = logging.getLogger("stolin.activity")


class ActivityLogMiddleware:
    """Write a compact page/API activity trail to a dedicated log file."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        started_at = time.perf_counter()
        response = self.get_response(request)
        elapsed_ms = (time.perf_counter() - started_at) * 1000

        activity_logger.info(
            "method=%s path=%s status=%s elapsed_ms=%.1f user=%s ip=%s referer=%s",
            request.method,
            request.get_full_path(),
            response.status_code,
            elapsed_ms,
            self._user_label(request),
            self._client_ip(request),
            request.META.get("HTTP_REFERER", "-"),
        )
        return response

    @staticmethod
    def _user_label(request):
        user = getattr(request, "user", None)
        if user and user.is_authenticated:
            return user.get_username()
        session_key = getattr(getattr(request, "session", None), "session_key", None)
        if session_key:
            return f"anonymous:{session_key[:8]}"
        return "anonymous"

    @staticmethod
    def _client_ip(request):
        forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
        if forwarded_for:
            return forwarded_for.split(",", 1)[0].strip()
        return request.META.get("REMOTE_ADDR", "-")
