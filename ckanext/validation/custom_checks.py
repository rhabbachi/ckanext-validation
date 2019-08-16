# encoding: utf-8
from goodtables import Error, spec
import ckantoolkit as t
from collections import namedtuple

spec['errors']['foreign-key'] = {
    "name": "Foreign Key Error",
    "type": "schema",
    "context": "body",
    "weight": 7,
    "message": 'Value in column {column_number} and row {row_number} is not found in the referenced data table: {resource_id}',
    "description": "Values in this field must be taken from the foreign key field in the referenced data table."
}


class ForeignKeyCheck(object):

    def __init__(self, **options):
        self.__foreign_fields_cache = None

    def check_row(self, cells):
        # Prepare cache
        if self.__foreign_fields_cache is None:
            self.__foreign_fields_cache = \
                ForeignKeyCheck._create_foreign_fields_cache(cells)

        # Step through the cells and check values are valid
        errors = []
        for cell in cells:

            default_field = namedtuple('field', 'descriptor')
            field = cell.get('field', default_field(descriptor={}))

            if field.descriptor.get('foreignKey'):

                valid_values = self.__foreign_fields_cache[cell['header']]['values']
                resource_id = self.__foreign_fields_cache[cell['header']]['resource_id']
                resource_url = self.__foreign_fields_cache[cell['header']]['resource_url']

                if not valid_values and cell['row-number'] == 1:
                    errors.append(Error(
                        'foreign-key',
                        cell,
                        row_number=cell['row-number'],
                        message="No foreign-key reference values found.  Have you uploaded/selected a valid resource?"
                    ))

                elif cell['value'] not in valid_values:

                    errors.append(Error(
                        'foreign-key',
                        cell,
                        row_number=cell['row-number'],
                        message_substitutions={
                            'resource_id': resource_id,
                            'resource_url': resource_url
                        }
                    ))

        return errors

    @staticmethod
    def _create_foreign_fields_cache(cells):
        cache = {}
        for column_number, cell in enumerate(cells, start=1):

            default_field = namedtuple('field', 'descriptor')
            field = cell.get('field', default_field(descriptor={}))
            foreign_key = field.descriptor.get('foreignKey')

            if foreign_key:

                # If field is a string, then it is a resource reference
                if isinstance(foreign_key, basestring):
                    res_id, field = tuple(foreign_key.split(':')[0:2])
                    res_url = ForeignKeyCheck._get_resource_url(res_id)
                    values = ForeignKeyCheck._get_valid_values(
                        res_id,
                        field
                    )
                # If field is a list, then the valid values already determined
                elif type(foreign_key) is list:
                    res_id = ""
                    res_url = ""
                    values = foreign_key
                else:
                    res_id = ""
                    res_url = ""
                    values = []

                cache[cell['header']] = {
                    'values': values,
                    'resource_id': res_id,
                    'resource_url': res_url
                }

        return cache

    @staticmethod
    def _get_resource_url(resource_id):
        # Ideally include a link to the referenced dataset in error report
        # TODO: Not quite figured out how to do this yet
        resource = t.get_action('resource_show')(
            # FIXME: Should we really ignore the auth here?
            {'ignore_auth': True},
            {'id': resource_id}
        )
        return resource.get('url', '')

    @staticmethod
    def _get_valid_values(resource_id, field):
        data_dict = {
            'resource_id': resource_id,
            'fields': [field],
            'limit': 1000
        }
        register_translator()
        result = t.get_action('datastore_search')(
            # FIXME: Should we really ignore the auth here?
            {'ignore_auth': True},
            data_dict
        )
        valid_values = [x[field] for x in result.get('records', [])]
        return valid_values


def register_translator():
    # Workaround until core translation function defaults to Flask
    from paste.registry import Registry
    from ckan.lib.cli import MockTranslator
    registry = Registry()
    registry.prepare()
    from pylons import translator
    registry.register(translator, MockTranslator())
