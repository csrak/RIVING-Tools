from django.apps import AppConfig

class FinDataClConfig(AppConfig):
    name = 'fin_data_cl'

    def ready(self):
        # Any initialization logic that doesn't involve database access can go here.
        # For instance, you might want to set up signals or do other non-database related tasks.
        pass
