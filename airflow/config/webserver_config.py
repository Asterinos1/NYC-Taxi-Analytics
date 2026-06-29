"""
webserver_config.py
===================
Custom Airflow webserver configuration for the local development sandbox.

Configures Flask-AppBuilder to support anonymous access mapping the public role
to 'Admin'. This enables automated UI screenshot capture (via Chrome headless)
without requiring login authentication.
"""

import os
from flask_appbuilder.security.manager import AUTH_DB

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

# Flask-AppBuilder configuration
AUTH_TYPE = AUTH_DB

# Enable anonymous access and map public role to Admin for easy screenshot capture
AUTH_ROLE_PUBLIC = "Admin"
