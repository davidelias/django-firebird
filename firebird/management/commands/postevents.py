import sys

from django.core.management.base import LabelCommand, CommandError
from django.core.mail import mail_admins
from django.conf import settings
from django.utils.importlib import import_module
from django.db import connections

from firebird.backend.base import Database

class Command(LabelCommand):
    def handle_label(self, label, **options):
        try:
            connection = self.get_connection(label)
        except Database.OperationalError, e:
            raise CommandError('Cannot connect to database "%s": %s' % (label, e))

        processors = self.get_event_processors(label)
        if not processors:
            raise CommandError('You must define at least one post event processors.')
    
        for event, processor in processors.items():
            if hasattr(processor, 'execute_on_run') and processor.execute_on_run == True:
                processor(connection, None)

        conduit = connection.event_conduit(processors.keys())
        while 1:
            try:
                post_events = conduit.wait()
                for event, posted in post_events.items():
                    if posted:
                        processors[event](connection, post_events)
            except Exception, e:
                import traceback

                connection.rollback()
                for conn in connections.all():
                    conn.close()
                #conduit.close()

                subject = 'Error: Firebird post events listener'
                message = 'Received events: %s\n\n%s' % (post_events,
                    '\n'.join(traceback.format_exception(*sys.exc_info())))
                mail_admins(subject, message, fail_silently=False)
            else:
                connection.commit()
                for conn in connections.all():
                    conn.close()
    
    def get_connection(self, alias):
        settings_dict = settings.DATABASES[alias]
        params = {}
        if settings_dict['NAME'] == '':
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured("You need to specify DATABASE.NAME in your Django settings file.")
        params['dsn'] = settings_dict['NAME']
        if settings_dict['HOST']:
            params['dsn'] = ('%s:%s') % (settings_dict['HOST'], params['dsn'])
        if settings_dict['PORT']:
            params['port'] = settings_dict['PORT']
        if settings_dict['USER']:
            params['user'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            params['password'] = settings_dict['PASSWORD']
        connection = Database.connect(**params)

        # Read only transaction
        connection.default_tpb = (
            Database.isc_tpb_read + \
            Database.isc_tpb_read_committed + \
            Database.isc_tpb_rec_version)

        return connection
    
    def get_event_processors(self, alias):
        processors = {}
        database = settings.DATABASES[alias]
        if database['ENGINE'] == 'firebird.backend':
            for event_name, path in database.get('EVENTS', {}).items():
                module, attr = path[:path.rfind('.')], path[path.rfind('.')+1:]
                try:
                    processors[event_name] = getattr(import_module(module), attr)
                except ImportError, e:
                    raise CommandError('Importing database event processor module "%s": %s' % (module, e))
        return processors