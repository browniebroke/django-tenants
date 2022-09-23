import sys

from django.db import transaction

from django.core.management import get_commands, load_command_class
from django.core.management.commands.migrate import Command as MigrateCommand
from django.db.migrations.recorder import MigrationRecorder

from django_tenants.signals import schema_migrated, schema_migrate_message
from django_tenants.utils import get_public_schema_name, get_tenant_database_alias


def load_base_migrate_command():
    """
    Load a custom migrate command currently used in the project.

    This is taken from the django.core.management.call_command function.
    It enables us to respect the Django command loading logic,
    and automatically pick a custom migrate command other than Django's.
    """
    from django_tenants.management.commands.migrate_schemas import MigrateSchemasCommand

    command_name = 'migrate'
    app_name = get_commands()[command_name]
    command_class = load_command_class(app_name, command_name)
    if isinstance(command_class, MigrateSchemasCommand):
        # The custom command is ours -> return the one from Django
        command_class = MigrateCommand
    return command_class


def run_migrations(args, options, executor_codename, schema_name, tenant_type='',
                   allow_atomic=True, idx=None, count=None):
    from django.core.management import color
    from django.core.management.base import OutputWrapper
    from django.db import connections
    style = color.color_style()

    def style_func(msg):
        percent_str = ''
        if idx is not None and count is not None and count > 0:
            percent_str = '%d/%d (%s%%) ' % (idx + 1, count, int(100 * (idx + 1) / count))

        message = '[%s%s:%s] %s' % (
            percent_str,
            style.NOTICE(executor_codename),
            style.NOTICE(schema_name),
            msg
        )
        signal_message = '[%s%s:%s] %s' % (
            percent_str,
            executor_codename,
            schema_name,
            msg
        )
        schema_migrate_message.send(run_migrations, message=signal_message)
        return message

    connection = connections[options.get('database', get_tenant_database_alias())]
    connection.set_schema(schema_name, tenant_type=tenant_type)

    # ensure that django_migrations table is created in the schema before migrations run, otherwise the migration
    # table in the public schema gets picked and no migrations are applied
    migration_recorder = MigrationRecorder(connection)
    migration_recorder.ensure_schema()

    stdout = OutputWrapper(sys.stdout)
    stdout.style_func = style_func
    stderr = OutputWrapper(sys.stderr)
    stderr.style_func = style_func
    if int(options.get('verbosity', 1)) >= 1:
        stdout.write(style.NOTICE("=== Starting migration"))
    migrate_command_class = load_base_migrate_command()
    migrate_command_class(stdout=stdout, stderr=stderr).execute(*args, **options)

    try:
        transaction.commit()
        connection.close()
        connection.connection = None
    except transaction.TransactionManagementError:
        if not allow_atomic:
            raise

        # We are in atomic transaction, don't close connections
        pass

    connection.set_schema_to_public()
    schema_migrated.send(run_migrations, schema_name=schema_name)


class MigrationExecutor:
    codename = None

    def __init__(self, args, options):
        self.args = args
        self.options = options

        self.PUBLIC_SCHEMA_NAME = get_public_schema_name()
        self.TENANT_DB_ALIAS = get_tenant_database_alias()

    def run_migrations(self, tenants=None):
        raise NotImplementedError

    def run_multi_type_migrations(self, tenants):
        raise NotImplementedError
