"""
Firebird database backend for Django.

Requires kinterbasdb: http://www.firebirdsql.org/index.php?op=devel&sub=python
"""
import datetime
try:
    from decimal import Decimal
except ImportError:
    from django.utils._decimal import Decimal
from django.utils.encoding import smart_str, smart_unicode

try:
    import kinterbasdb as Database
    import kinterbasdb.typeconv_datetime_stdlib as typeconv_dt
    import kinterbasdb.typeconv_fixed_decimal as typeconv_fd
    import kinterbasdb.typeconv_text_unicode as typeconv_tu
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading kinterbasdb module: %s" % e)

from django.db.backends import *

from client import DatabaseClient
from creation import DatabaseCreation
from introspection import DatabaseIntrospection

DB_CHARSET_TO_DB_CHARSET_CODE = typeconv_tu.DB_CHAR_SET_NAME_TO_DB_CHAR_SET_ID_MAP
DB_CHARSET_TO_PYTHON_CHARSET = typeconv_tu.DB_CHAR_SET_NAME_TO_PYTHON_ENCODING_MAP

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
OperationalError = Database.OperationalError

class DatabaseFeatures(BaseDatabaseFeatures):
    can_return_id_from_insert = True
    uses_savepoints = False

class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "firebird.backend.compiler"

    def __init__(self, *args, **kwargs):
        super(DatabaseOperations, self).__init__(*args, **kwargs)
        self._engine_version = None
    
    def _get_engine_version(self):
        """ 
        Access method for engine_version property.
        engine_version return a full version in string format 
        (ie: 'WI-V6.3.5.4926 Firebird 1.5' )
        """
        if self._engine_version is None:
            from django.db import connection            
            self._engine_version = connection.get_server_version()
        return self._engine_version
    engine_version = property(_get_engine_version)
    
    def _get_firebird_version(self):
        """ 
        Access method for firebird_version property.
        firebird_version return the version number in a object list format
        Useful for ask for just a part of a version number, for instance, major version is firebird_version[0]  
        """
        return [int(val) for val in self.engine_version.split()[-1].split('.')]
    firebird_version = property(_get_firebird_version)

    def autoinc_sql(self, table, column):
        # To simulate auto-incrementing primary keys in Firebird, we have to create a generator and a trigger.
        gn_name = self.quote_name(self.get_generator_name(table))
        tr_name = self.quote_name(self.get_trigger_name(table))
        tbl_name = self.quote_name(table)
        col_name = self.quote_name(column)
        generator_sql = """CREATE GENERATOR %(gn_name)s""" % locals()
        trigger_sql = """
            CREATE TRIGGER %(tr_name)s FOR %(tbl_name)s
            BEFORE INSERT
            AS 
            BEGIN
               IF (NEW.%(col_name)s IS NULL) THEN 
                   NEW.%(col_name)s = GEN_ID(%(gn_name)s, 1);
            END""" % locals()
        return generator_sql, trigger_sql

    def date_extract_sql(self, lookup_type, field_name):
        # Firebird uses WEEKDAY keyword.
        lkp_type = lookup_type
        if lkp_type == 'week_day':
            lkp_type = 'weekday'            
        return "EXTRACT(%s FROM %s)" % (lkp_type.upper(), field_name)

    def date_trunc_sql(self, lookup_type, field_name):
        if lookup_type == 'year':
            sql = "EXTRACT(year FROM %s)||'-01-01 00:00:00'" % field_name
        elif lookup_type == 'month':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-01 00:00:00'" % (field_name, field_name)
        elif lookup_type == 'day':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-'||EXTRACT(day FROM %s)||' 00:00:00'" % (field_name, field_name, field_name)
        return "CAST(%s AS TIMESTAMP)" % sql
    
    def lookup_cast(self, lookup_type):
        #if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
        if lookup_type in ('iexact', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"
    
    def fulltext_search_sql(self, field_name):
        # We use varchar for TextFields so this is possible
        # Look at http://www.volny.cz/iprenosil/interbase/ip_ib_strings.htm
        return '%%s CONTAINING %s' % self.quote_name(field_name)

    def last_insert_id(self, cursor, table_name, pk_name):
        cursor.execute('SELECT GEN_ID(%s, 0) FROM rdb$database' % (self.get_generator_name(table_name),))
        return cursor.fetchone()[0]

    def max_name_length(self):
        return 31

    def convert_values(self, value, field):
        return super(DatabaseOperations, self).convert_values(value, field)
            
    def quote_name(self, name):
        if not name.startswith('"') and not name.endswith('"'):
            name = '"%s"' % util.truncate_name(name, self.max_name_length())
        return name.upper()
    
    def return_insert_id(self):
        return "RETURNING %s", ()
    
    def savepoint_create_sql(self, sid):
        return "SAVEPOINT " + self.quote_name(sid)

    def savepoint_rollback_sql(self, sid):
        return "ROLLBACK TO " + self.quote_name(sid)

    def get_generator_name(self, table_name):
        return '%s_GN' % util.truncate_name(table_name, self.max_name_length() - 3).upper()

    def get_trigger_name(self, table_name):
        name_length = DatabaseOperations().max_name_length() - 3
        return '%s_TR' % util.truncate_name(table_name, self.max_name_length() - 3).upper()

class DatabaseValidation(BaseDatabaseValidation):
    pass

class TypeTranslator(object):
    db_charset_code = None
    charset = None
    
    def set_charset(self, db_charset):
        self.db_charset_code = DB_CHARSET_TO_DB_CHARSET_CODE[db_charset]
        self.charset = DB_CHARSET_TO_PYTHON_CHARSET[db_charset]
    
    @property
    def type_translate_in(self):
        return {
            'DATE': self.in_date,
            'TIME': self.in_time,
            'TIMESTAMP': self.in_timestamp,
            'INTEGER': self.in_integer,
            'FIXED': self.in_fixed,
            'TEXT': self.in_text,
            'TEXT_UNICODE': self.in_unicode,
            'BLOB': self.in_blob
        }
    
    @property
    def type_translate_out(self):
        return {
            'DATE': typeconv_dt.date_conv_out,
            'TIME': typeconv_dt.time_conv_out,
            'TIMESTAMP': typeconv_dt.timestamp_conv_out,
            'FIXED': typeconv_fd.fixed_conv_out_precise,
            'TEXT': self.out_text,
            'TEXT_UNICODE': self.out_unicode,
            'BLOB': self.out_blob
        }
    
    def in_date(self, value):
        if isinstance(value, basestring):
            #Replaces 6 digits microseconds to 4 digits allowed in Firebird
            value = value[:24]
        return typeconv_dt.date_conv_in(value)
    
    def in_time(self, value):
        if isinstance(value, datetime.datetime):
            value = datetime.time(value.hour, value.minute, value.second, value.microsecond)
        return typeconv_dt.time_conv_in(value)
    
    def in_timestamp(self, value):
        if isinstance(value, basestring):
            #Replaces 6 digits microseconds to 4 digits allowed in Firebird
            value = value[:24]
        return typeconv_dt.timestamp_conv_in(value)

    def in_integer(self, value):
        return value
    
    def in_fixed(self, (value, scale)):
        if value is not None:
            if isinstance(value, basestring):
                value = Decimal(value)
            return typeconv_fd.fixed_conv_in_precise((value, scale))
    
    def in_text(self, text):
        if text is not None:  
            return smart_str(text, encoding=self.charset)
    
    def in_unicode(self, (text, charset)):
        if text is not None:
            return typeconv_tu.unicode_conv_in((smart_unicode(text), self.db_charset_code))

    def in_blob(self, text): 
        return typeconv_tu.unicode_conv_in((smart_unicode(text), self.db_charset_code))
    
    def out_text(self, text):
        if text is not None:
            return smart_unicode(text, encoding=self.charset)
        return text
    
    def out_unicode(self, (text, charset)):
        return typeconv_tu.unicode_conv_out((text, self.db_charset_code))

    def out_blob(self, text):
        return typeconv_tu.unicode_conv_out((text, self.db_charset_code))

class DatabaseWrapper(BaseDatabaseWrapper):

    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': "LIKE %s ESCAPE'\\'",
        'icontains': 'CONTAINING %s', #case is ignored
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'STARTING WITH %s', #looks to be faster then LIKE
        'endswith': "LIKE %s ESCAPE'\\'",
        'istartswith': 'STARTING WITH UPPER(%s)',
        'iendswith': "LIKE UPPER(%s) ESCAPE'\\'",
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        
        self._server_version = None
        self._type_translator = TypeTranslator()
        
        self.features = DatabaseFeatures()
        self.ops = DatabaseOperations()
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = DatabaseValidation(self)

    def _cursor(self):
        if self.connection is None:
            settings_dict = self.settings_dict
            if settings_dict['NAME'] == '':
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured("You need to specify DATABASE_NAME in your Django settings file.")
            conn_params = {
                'charset': 'UNICODE_FSS'
            }
            conn_params['dsn'] = settings_dict['NAME']
            if settings_dict['HOST']:
                conn_params['dsn'] = ('%s:%s') % (settings_dict['HOST'], conn_params['dsn'])
            if settings_dict['PORT']:
                conn_params['port'] = settings_dict['PORT']
            if settings_dict['USER']:
                conn_params['user'] = settings_dict['USER']
            if settings_dict['PASSWORD']:
                conn_params['password'] = settings_dict['PASSWORD']
            conn_params.update(settings_dict['OPTIONS'])
            self.connection = Database.connect(**conn_params)
            self._type_translator.set_charset(self.connection.charset)
        return FirebirdCursorWrapper(self.connection.cursor(), self._type_translator)
    
    def get_server_version(self):
        if not self._server_version:
            if not self.connection:
                self.cursor()
            self._server_version = self.connection.server_version
        return self._server_version

class FirebirdCursorWrapper(object):
    """
    Django uses "format" style placeholders, but firebird uses "qmark" style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".
    
    We need to do some data translation too.
    See: http://kinterbasdb.sourceforge.net/dist_docs/usage.html for Dynamic Type Translation
    """
    
    def __init__(self, cursor, type_translator):
        self.cursor = cursor
        self.cursor.set_type_trans_in(type_translator.type_translate_in)
        self.cursor.set_type_trans_out(type_translator.type_translate_out)
    
    def execute(self, query, params=()):
        cquery = self.convert_query(query, len(params))
        try:
            return self.cursor.execute(cquery, params)
        except Database.ProgrammingError, e:
            err_no = int(str(e).split()[0].strip(',()'))
            output = ["Execute query error. FB error No. %i" % err_no]
            output.extend(str(e).split("'")[1].split('\\n'))
            output.append("Query:")
            output.append(cquery)
            output.append("Parameters:")
            output.append(str(params))
            if err_no in (-803,):
                raise IntegrityError("\n".join(output))
            raise DatabaseError("\n".join(output))

    def executemany(self, query, param_list):
        try:
            query = self.convert_query(query, len(param_list[0]))
            return self.cursor.executemany(query, param_list)
        except (IndexError,TypeError):
            return None

    def convert_query(self, query, num_params):
        return query % tuple("?" * num_params)
    
    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)


