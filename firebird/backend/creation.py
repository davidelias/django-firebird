import sys, time
import kinterbasdb as Database

from django.db.backends.creation import BaseDatabaseCreation, TEST_DATABASE_PREFIX

class DatabaseCreation(BaseDatabaseCreation):
    # This dictionary maps Field objects to their associated Firebird column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    #
    # Any format strings starting with "qn_" are quoted before being used in the
    # output (the "qn_" prefix is stripped before the lookup is performed.

    data_types = {
        'AutoField':         'integer',
        'BooleanField':      'integer',
        'CharField':         'varchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
        'DateField':         'date',
        'DateTimeField':     'timestamp',
        'DecimalField':      'numeric(%(max_digits)s, %(decimal_places)s)',
        'FileField':         'varchar(%(max_length)s)',
        'FilePathField':     'varchar(%(max_length)s)',
        'FloatField':        'double precision',
        'IntegerField':      'integer',
        'IPAddressField':    'char(15)',
        'NullBooleanField':  'integer',
        'OneToOneField':     'integer',
        'PositiveIntegerField': 'integer %% CHECK (%(qn_column)s >= 0)',
        'PositiveSmallIntegerField': 'smallint %% CHECK (%(qn_column)s >= 0)',
        'SlugField':         'varchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField':         'blob sub_type 1',
        'TimeField':         'time',
    }

    def sql_create_model(self, model, style, known_models=set()):
        """
        Returns the SQL required to create a single model, as a tuple of:
            (list_of_sql, pending_references_dict)
        """
        from django.db import models
        opts = model._meta
        if not opts.managed:
            return [], {}
        final_output = []
        table_output = []
        pending_references = {}
        qn = self.connection.ops.quote_name
        for f in opts.local_fields:
            col_type = f.db_type()
            tablespace = f.db_tablespace or opts.db_tablespace
            if col_type is None:
                # Skip ManyToManyFields, because they're not represented as
                # database columns in this table.
                continue
            # Make the definition (e.g. 'foo VARCHAR(30)') for this field.
            field_output = [style.SQL_FIELD(qn(f.column)), style.SQL_COLTYPE(col_type)]          
            if not f.null:
                # Workaraund  for Firebird 1.5 : the NOT NULL keyword should be located after field constraint definition
                # Mark '%' is 'NOT NULL' placeholder
                if field_output[-1].find('%') == -1:
                    field_output.append(style.SQL_KEYWORD('NOT NULL'))
                else:
                    field_output[-1] = field_output[-1].replace('%', style.SQL_KEYWORD('NOT NULL'))
            elif field_output[-1].find('%'):    # Erase placeholder
                field_output[-1] = field_output[-1].replace('%','')
            if f.primary_key:
                field_output.append(style.SQL_KEYWORD('PRIMARY KEY'))
            elif f.unique:
                field_output.append(style.SQL_KEYWORD('UNIQUE'))
            if tablespace and f.unique:
                # We must specify the index tablespace inline, because we
                # won't be generating a CREATE INDEX statement for this field.
                field_output.append(self.connection.ops.tablespace_sql(tablespace, inline=True))
            if f.rel:
                ref_output, pending = self.sql_for_inline_foreign_key_references(f, known_models, style)
                if pending:
                    pr = pending_references.setdefault(f.rel.to, []).append((model, f))
                else:
                    field_output.extend(ref_output)
            table_output.append(' '.join(field_output))
        if opts.order_with_respect_to:
            table_output.append(style.SQL_FIELD(qn('_order')) + ' ' + \
                style.SQL_COLTYPE(models.IntegerField().db_type()))
        for field_constraints in opts.unique_together:
            table_output.append(style.SQL_KEYWORD('UNIQUE') + ' (%s)' % \
                ", ".join([style.SQL_FIELD(qn(opts.get_field(f).column)) for f in field_constraints]))

        full_statement = [style.SQL_KEYWORD('CREATE TABLE') + ' ' + style.SQL_TABLE(qn(opts.db_table)) + ' (']
        for i, line in enumerate(table_output): # Combine and add commas.
            full_statement.append('    %s%s' % (line, i < len(table_output)-1 and ',' or ''))
        full_statement.append(')')
        if opts.db_tablespace:
            full_statement.append(self.connection.ops.tablespace_sql(opts.db_tablespace))
        full_statement.append(';')
        final_output.append('\n'.join(full_statement))

        if opts.has_auto_field:
            # Add any extra SQL needed to support auto-incrementing primary keys.
            auto_column = opts.auto_field.db_column or opts.auto_field.name
            autoinc_sql = self.connection.ops.autoinc_sql(opts.db_table, auto_column)
            if autoinc_sql:
                for stmt in autoinc_sql:
                    final_output.append(stmt)

        return final_output, pending_references
    
    def _get_connection_params(self, **overrides):
        settings_dict = self.connection.settings_dict
        conn_params = {
            'charset': 'UNICODE_FSS'
        }
        conn_params['database'] = settings_dict['NAME']
        if settings_dict['HOST']:
            conn_params['host'] = settings_dict['HOST']
        if settings_dict['PORT']:
            conn_params['port'] = settings_dict['PORT']
        if settings_dict['USER']:
            conn_params['user'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']
        conn_params.update(settings_dict['OPTIONS'])
        conn_params.update(overrides)
        return conn_params
    
    def _rollback_works(self):
        cursor = self.connection.cursor()
        cursor.execute('CREATE TABLE ROLLBACK_TEST (X INT)')
        self.connection._commit()
        cursor.execute('INSERT INTO ROLLBACK_TEST (X) VALUES (8)')
        self.connection._rollback()
        cursor.execute('SELECT COUNT(X) FROM ROLLBACK_TEST')
        count, = cursor.fetchone()
        #cursor.execute('DROP TABLE ROLLBACK_TEST')
        #self.connection._commit()
        return count == 0
    
    def _create_test_db(self, verbosity, autoclobber):
        "Internal implementation - creates the test db tables."
        suffix = self.sql_table_creation_suffix()

        if self.connection.settings_dict['TEST_NAME']:
            test_database_name = self.connection.settings_dict['TEST_NAME']
        else:
            test_database_name = TEST_DATABASE_PREFIX + self.connection.settings_dict['NAME']

        qn = self.connection.ops.quote_name
        
        try:
            self._create_database(test_database_name)
        except Exception, e:
            sys.stderr.write("Got an error creating the test database: %s\n" % e)
            if not autoclobber:
                confirm = raw_input("Type 'yes' if you would like to try deleting the test database '%s', or 'no' to cancel: " % test_database_name)
            if autoclobber or confirm == 'yes':
                try:
                    if verbosity >= 1:
                        print "Destroying old test database..."
                    self._destroy_test_db(test_database_name, verbosity)
                    if verbosity >= 1:
                        print "Creating test database..."
                    self._create_database(test_database_name)
                except Exception, e:
                    sys.stderr.write("Got an error recreating the test database: %s\n" % e)
                    sys.exit(2)
            else:
                print "Tests cancelled."
                sys.exit(1)

        return test_database_name
    
    def _create_database(self, test_database_name):
        params = self._get_connection_params(database=test_database_name)
        connection = Database.create_database( \
            "CREATE DATABASE '%(database)s' user '%(user)s' password '%(password)s'" % params)
        #connection.close()

    def _destroy_test_db(self, test_database_name, verbosity):
        connection = Database.connect(**self._get_connection_params(database=test_database_name))
        connection.drop_database()
        connection.close()

