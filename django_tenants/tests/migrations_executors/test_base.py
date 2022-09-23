from unittest import mock

from django_tenants.migration_executors.base import load_base_migrate_command
from django_tenants.test.cases import TenantTestCase
from django.core.management.commands.migrate import Command as MigrateCommand


class LoadBaseMigrateCommandTestCase(TenantTestCase):

    def test_load_base_migrate_command_default(self):
        command_class = load_base_migrate_command()
        self.assertIs(command_class, MigrateCommand)

    def test_load_base_migrate_command(self):
        class CustomMigrateCommand(MigrateCommand):
            pass

        with mock.patch(
            'django_tenants.management.commands.migrate.Command',
            CustomMigrateCommand,
        ):
            command = load_base_migrate_command()
            self.assertIsInstance(command, CustomMigrateCommand)
