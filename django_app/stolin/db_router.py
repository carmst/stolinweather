class WeatherAppRouter:
    """Route mirrored weather/Kalshi tables to Supabase.

    Django's built-in auth/session/admin tables stay on the local SQLite
    default database. The `markets` app mirrors production pipeline tables and
    is read from the `weather` database alias.
    """

    app_label = "markets"

    def db_for_read(self, model, **hints):
        if model._meta.app_label == self.app_label:
            return "weather"
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == self.app_label:
            return "weather"
        return None

    def allow_relation(self, obj1, obj2, **hints):
        labels = {obj1._meta.app_label, obj2._meta.app_label}
        if self.app_label in labels:
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if app_label == self.app_label:
            return False
        return db == "default"
