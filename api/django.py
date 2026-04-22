from __future__ import annotations

import os
import sys
from pathlib import Path


REPO_DIR = Path(__file__).resolve().parent.parent
DJANGO_DIR = REPO_DIR / "django_app"

sys.path.insert(0, str(DJANGO_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "stolin.settings")

from django.core.wsgi import get_wsgi_application  # noqa: E402


app = get_wsgi_application()
application = app
