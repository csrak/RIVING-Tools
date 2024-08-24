import os
from django.core.management.base import BaseCommand
from django.conf import settings
import shutil


class Command(BaseCommand):
    help = 'Create or delete apps and automatically update INSTALLED_APPS in settings.py'

    def add_arguments(self, parser):
        parser.add_argument('action', type=str, choices=['create', 'delete'], help='Action to perform: create or delete')
        parser.add_argument('app_name', type=str, help='Name of the app to create or delete')

    def handle(self, *args, **options):
        action = options['action']
        app_name = options['app_name']

        if action == 'create':
            self.create_app(app_name)
        elif action == 'delete':
            self.delete_app(app_name)

    def create_app(self, app_name):
        # Run the startapp command
        os.system(f'django-admin startapp {app_name}')

        # Add app to INSTALLED_APPS in settings.py
        settings_path = os.path.join(settings.BASE_DIR, 'finriv', 'settings.py')
        with open(settings_path, 'r') as file:
            lines = file.readlines()

        for i, line in enumerate(lines):
            if 'INSTALLED_APPS' in line:
                break

        lines.insert(i + 1, f"    '{app_name}',\n")

        with open(settings_path, 'w') as file:
            file.writelines(lines)

        self.stdout.write(self.style.SUCCESS(f"App '{app_name}' created and added to INSTALLED_APPS."))

    def delete_app(self, app_name):
        # Remove the app directory using shutil
        app_dir = os.path.join(settings.BASE_DIR, app_name)
        if os.path.exists(app_dir):
            shutil.rmtree(app_dir)  # This works on all platforms, including Windows
        else:
            self.stdout.write(self.style.ERROR(f"App '{app_name}' does not exist."))
            return

        # Remove app from INSTALLED_APPS in settings.py
        settings_path = os.path.join(settings.BASE_DIR, 'finriv', 'settings.py')
        with open(settings_path, 'r') as file:
            lines = file.readlines()

        with open(settings_path, 'w') as file:
            for line in lines:
                if line.strip() != f"'{app_name}',":
                    file.write(line)

        self.stdout.write(self.style.SUCCESS(f"App '{app_name}' deleted and removed from INSTALLED_APPS."))