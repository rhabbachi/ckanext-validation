# encoding: utf-8
from goodtables import Error, spec
import logging
import ckantoolkit as t

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
            if cell['field'].descriptor.get('foreignKey'):
                valid_values = self.__foreign_fields_cache[cell['header']]['values']
                resource_id = self.__foreign_fields_cache[cell['header']]['resource_id']
                resource_url = self.__foreign_fields_cache[cell['header']]['resource_url']
                if cell['value'] not in valid_values:
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
            field = cell.get('field')
            if field is not None:
                foreign_key = field.descriptor.get('foreignKey')
                if foreign_key:
                    res_id, field = tuple(foreign_key.split(':')[0:2])
                    values = ForeignKeyCheck._get_valid_values(
                        res_id,
                        field
                    )
                    header = cell['header']
                    res_url = ForeignKeyCheck._get_resource_url(res_id)
                    cache[header] = {
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
            'fields': [field]
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
