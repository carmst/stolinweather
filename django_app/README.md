# Stolin Weather Django Control Room

This Django project is a Python-first control room for the existing weather/Kalshi pipeline. It is intentionally isolated from the current Vercel frontend and does not change production routing.

## What It Does

- Reads `DATABASE_URL` from `../.env.runtime`.
- Connects mirrored weather/Kalshi models to the same Supabase/Postgres database used by the current pipeline.
- Keeps Django-owned auth/session/admin tables in local SQLite so setup does not write framework tables into Supabase.
- Mirrors the existing `app.*` tables with unmanaged Django models.
- Registers those mirrored tables in Django admin as read-only views.
- Provides a small index page with row counts for the core tables.

## What It Does Not Do Yet

- It does not replace the existing frontend.
- It does not own database migrations for the current `app.*` schema.
- It does not write to market, forecast, score, bet, or settlement tables from admin.

## Setup

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run Django checks:

```bash
cd django_app
python manage.py check
```

Create local Django admin/auth tables:

```bash
python manage.py migrate
```

Create a local Django admin user:

```bash
python manage.py createsuperuser
```

Start the local Django server:

```bash
python manage.py runserver 127.0.0.1:8000
```

Then open:

```text
http://127.0.0.1:8000/
http://127.0.0.1:8000/admin/
```

## Logs

Django writes categorized local logs under `django_app/logs/`:

```text
django_app/logs/activity.log  # pages/API paths viewed, status codes, timing, user/session, IP, referrer
django_app/logs/django.log    # Django/server/app information
django_app/logs/errors.log    # errors and exceptions
```

Useful commands:

```bash
tail -f logs/activity.log
tail -f logs/django.log
tail -f logs/errors.log
```

The `logs/` directory is ignored by git.

## Current Design

The app in `markets/` maps the existing Supabase schema into Django models with `managed = False`. A database router sends `markets` reads to the `weather` Supabase database alias and sends Django auth/session/admin tables to local SQLite.

This gives us a safe bridge: the current collection/scoring scripts keep running, while Django becomes the Python-native place to inspect and eventually rebuild the application.
