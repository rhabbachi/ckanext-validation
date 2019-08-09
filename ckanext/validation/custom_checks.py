# encoding: utf-8
from goodtables import check, Error
import logging
import ckantoolkit as t


class ForeignKeyCheck(object):

    def __init__(self, **options):
        logging.warning("INITIALISING FOREIGN KEY CHECK")
        self.__foreign_fields_cache = None

    def check_row(self, cells):
        logging.warning("CHECKING ROW")
        # Prepare cache
        if self.__foreign_fields_cache is None:
            self.__foreign_fields_cache = \
                ForeignKeyCheck._create_foreign_fields_cache(cells)

        # Step through the cells and check values are valid
        errors = []
        for cell in cells:
            if cell['field'].descriptor.get('foreignKey'):
                logging.warning("Cell has foreign key")
                valid_values = self.__foreign_fields_cache[cell['header']]
                if cell['value'] not in valid_values:
                    message = (
                        'Foreign Key Error: Value in column {column_number} ' +
                        'and row {row_number} not a valid value'
                    )
                    error = Error(
                        'custom-error',
                        cell,
                        message=message
                    )
                    errors.append(error)
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
        result = t.get_action('datastore_search')(None, data_dict)
        logging.warning(result)
        logging.warning(result.records)
        return ['id1', 'id2', 'id3']
