# encoding: utf-8
from goodtables import Error, spec
import logging
import ckantoolkit as t

spec['errors']['foreign-key'] = {
    "name": "Foreign Key Error",
    "type": "schema",
    "context": "body",
    "weight": 7,
    "message": 'Foreign Key Error: Value in column {column_number} and row {row_number} not a valid value',
    "description": "This value must be exactly one of the values from the foreign reference."
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
            if cell['field'].descriptor.get('foreignKey'):
                valid_values = self.__foreign_fields_cache[cell['header']]
                if cell['value'] not in valid_values:
                    errors.append(Error(
                        'foreign-key',
                        cell,
                        row_number=cell['row-number']
                    ))
        return errors

    @staticmethod
    def _create_foreign_fields_cache(cells):
        cache = {}
        for column_number, cell in enumerate(cells, start=1):
            field = cell.get('field')
            if field is not None:
                foreign_key = field.descriptor.get('foreignKey')
                if foreign_key:
                    values = ForeignKeyCheck._get_valid_values(foreign_key)
                    header = cell['header']
                    cache[header] = values
        return cache

    @staticmethod
    def _get_valid_values(foreign_key):
        resource_id, field = tuple(foreign_key.split(':')[0:2])
        data_dict = {
            'resource_id': resource_id,
            'fields': [field]
        }
        register_translator()
        result = t.get_action('datastore_search')(
            # FIXME: Should we really ignore the auth here?
            {'ignore_auth': True},
            data_dict
        )
        valid_values = [x[field] for x in result.get('records', [])]
        logging.warning(valid_values)
        return valid_values


def register_translator():
    # Workaround until core translation function defaults to Flask
    from paste.registry import Registry
    from ckan.lib.cli import MockTranslator
    registry = Registry()
    registry.prepare()
    from pylons import translator
    registry.register(translator, MockTranslator())
