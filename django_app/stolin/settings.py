"""Django settings for a Python-first Stolin Weather control room.

This project is intentionally separate from the current Vercel frontend. It
connects to the same Supabase/Postgres database and mirrors existing tables
without taking ownership of migrations yet.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse


BASE_DIR = Path(__file__).resolve().parent.parent
REPO_DIR = BASE_DIR.parent
IS_VERCEL = os.environ.get("VERCEL") == "1"
LOG_DIR = Path("/tmp/stolin-django-logs") if IS_VERCEL else BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_env_file(REPO_DIR / ".env.runtime")


SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "local-dev-only-change-before-public-use")
DEBUG = os.environ.get("DJANGO_DEBUG", "1") != "0"
ALLOWED_HOSTS = [
    host.strip()
    for host in os.environ.get("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost,testserver,.vercel.app").split(",")
    if host.strip()
]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.postgres",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "markets",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "stolin.middleware.ActivityLogMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "stolin.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "stolin.wsgi.application"


def database_from_url(database_url: str | None) -> dict:
    if not database_url:
        return {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        }

    parsed = urlparse(database_url)
    query = parse_qs(parsed.query)

    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": parsed.path.lstrip("/") or "postgres",
        "USER": parsed.username or "",
        "PASSWORD": parsed.password or "",
        "HOST": parsed.hostname or "",
        "PORT": str(parsed.port or 5432),
        "OPTIONS": {
            key: values[-1]
            for key, values in query.items()
            if values and key in {"sslmode", "connect_timeout", "application_name"}
        },
        "CONN_MAX_AGE": int(os.environ.get("DJANGO_WEATHER_DB_CONN_MAX_AGE", "60")),
    }


DATABASES = {
    # Django-owned tables live locally for now. This keeps auth/session/admin
    # setup from writing framework tables into the production Supabase database.
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": Path("/tmp/django_control_room.sqlite3") if IS_VERCEL else BASE_DIR / "django_control_room.sqlite3",
    },
    # Existing weather/Kalshi pipeline tables live in Supabase/Postgres.
    "weather": database_from_url(os.environ.get("DATABASE_URL")),
}

DATABASE_ROUTERS = ["stolin.db_router.WeatherAppRouter"]

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "America/New_York"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Existing Supabase tables are canonical for now. Keep Django migrations focused
# on Django-owned tables, not mirrored pipeline data.
MIGRATION_MODULES = {
    "markets": None,
}

# Two mirrored forecast tables use composite primary keys in Postgres. Django
# has no native composite primary key in 4.2, so the read-only mirror marks one
# field as the admin identity and silences the resulting ForeignKey warning.
SILENCED_SYSTEM_CHECKS = ["fields.W342"]

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        },
        "activity": {
            "format": "%(asctime)s %(message)s",
        },
    },
    "handlers": {
        "django_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOG_DIR / "django.log",
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 5,
            "formatter": "standard",
        },
        "activity_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOG_DIR / "activity.log",
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 10,
            "formatter": "activity",
        },
        "errors_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOG_DIR / "errors.log",
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 10,
            "formatter": "standard",
            "level": "ERROR",
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
    },
    "loggers": {
        "django": {
            "handlers": ["django_file", "errors_file", "console"],
            "level": "INFO",
            "propagate": False,
        },
        "django.request": {
            "handlers": ["errors_file", "console"],
            "level": "ERROR",
            "propagate": False,
        },
        "stolin.activity": {
            "handlers": ["activity_file"],
            "level": "INFO",
            "propagate": False,
        },
        "markets": {
            "handlers": ["django_file", "errors_file", "console"],
            "level": "INFO",
            "propagate": False,
        },
    },
}
