# encoding: utf-8
from goodtables import Error, spec
import ckantoolkit as t
from collections import namedtuple
import logging
from goodtables.checks.constraints_checks import create_check_constraint

log = logging.getLogger(__name__)

spec['errors']['foreign-key'] = {
    "name": "Foreign Key Error",
    "type": "schema",
    "context": "body",
    "weight": 7,
    "message": 'Value in column {column_number} and row {row_number} is not found in the referenced data table: {resource_id}',
    "description": "Values in this field must be taken from the foreign key field in the referenced data table."
}

spec['errors']['missing-geometry'] = {
    "name": "Geometry Error",
    "type": "schema",
    "context": "body",
    "weight": 7,
    "message": 'There is no geometry specified for row {row_number}.',
    "description": "Every record in a geometry file, must include geometry co-ordinates."
}

spec['errors']["enumerable-constraint"] = {
    "name": "Invalid Value",
    "type": "schema",
    "context": "body",
    "weight": 7,
    "message": "The value {value} in row {row_number} and column {column_number} is not found in the list of valid values for this field: {constraint}",
    "description": "This field value should be equal to one of the values in the list of valid values.\n\n How it could be resolved:\n - If this value is not correct, update the value.\n - If value is correct, then remove or refine the `enum` constraint in the schema.\n - If this error should be ignored disable `enumerable-constraint` check in {validator}."
}


def enumerable_constraint(cells):
    errors = []
    log.warning("Called enumerable constraint")

    for cell in cells:
        log.debug("Enumerable Constraint: Cell {}".format(cell))
        field = cell.get('field')
        value = cell.get('value').encode('utf-8')
        log.warning("Field: {} Value: {}".format(field, value))

        # Skip if cell has no field
        if field is None:
            continue

        # Check constraint
        valid = field.test_value(value, constraints=['enum'])
        log.warning('Valid: {}'.format(valid))

        # Add error
        if not valid:
            message_substitutions = {
                'value': '"{}"'.format(value),
                'constraint': '"{}"'.format('", "'.join(field.constraints['enum']))
            }
            log.warning("Message substitutions: {}".format(message_substitutions))
            error = Error(
                'enumerable-constraint',
                cell,
                message_substitutions=message_substitutions
            )
            errors.append(error)

    log.debug('Enumerable constraint: Finished checking row')
    return errors


class UniqueConstraint(object):
    """
    We create a copy of the GoodTables uniqueness check so that we can
    issue a more specific error message whe composite uniqueness check fails.
    """

    def __init__(self, **options):
        self.__unique_fields_cache = None

    def check_row(self, cells):
        log.debug('Checking unique constraint')
        errors = []

        # Prepare unique checks
        if self.__unique_fields_cache is None:
            self.__unique_fields_cache = _create_unique_fields_cache(cells)

        # Check unique
        for column_numbers, cache in self.__unique_fields_cache.items():
            column_cells = tuple(
                cell
                for column_number, cell in enumerate(cells, start=1)
                if column_number in column_numbers
            )
            column_values = tuple(cell.get('value') for cell in column_cells)
            row_number = column_cells[0]['row-number']

            all_values_are_none = (set(column_values) == {None})
            if not all_values_are_none:
                if column_values in cache['data']:
                    # Custom code =============================================
                    message_substitutions = {
                        'row_numbers': str(row_number),
                    }
                    if len(column_numbers) == 1:
                        error = Error(
                            'unique-constraint',
                            column_cells[0],
                            message_substitutions=message_substitutions
                        )
                    else:
                        error = Error(
                            'unique-constraint',
                            column_cells[0],
                            message="Rows {row_numbers} have a composite uniqueness constraint violation. Primary key fields must form a unique combination in the dataset.",
                            message_substitutions=message_substitutions
                        )
                    # End Custom code =========================================
                    errors.append(error)
                cache['data'].add(column_values)
                cache['refs'].append(row_number)

        return errors


# Internal
def _create_unique_fields_cache(cells):
    primary_key_column_numbers = []
    cache = {}

    # Unique
    for column_number, cell in enumerate(cells, start=1):
        field = cell.get('field')
        if field is not None:
            if field.descriptor.get('primaryKey'):
                primary_key_column_numbers.append(column_number)
            if field.constraints.get('unique'):
                cache[tuple([column_number])] = {
                    'data': set(),
                    'refs': [],
                }

    # Primary key
    if primary_key_column_numbers:
        cache[tuple(primary_key_column_numbers)] = {
            'data': set(),
            'refs': [],
        }

    return cache


def geometry_check(cells):
    """
    Certain file types come with built in geometry e.g. SHP and GeoJSON. For
    these file types we prepare them with an adr_geometry_exists column, and
    here require that column to be truthy if the file is to pass validation.  This
    """
    log.debug('Geometry Check Called')

    for cell in cells:
        # Only run check if an adr_geometry_exists column in data
        if cell['header'] == 'adr_geometry_check':
            # If the geometry is falsy (including strings "False") then error
            if not cell['value'] or (cell['value'] in ["False", "false"]):
                log.debug('Missing geometry in row ' + str(cell['row-number']))
                return [Error(
                    'missing-geometry',
                    None,
                    row_number=cell['row-number']
                )]
            # If geometry exists
            else:
                log.debug('Found geometry in cell: ' +
                          str(cell) + " - " + str(cell['value']))
                return []
    else:
        log.debug("No geometry in this dataset")
        return []


class ForeignKeyCheck(object):

    def __init__(self, **options):
        self.__foreign_fields_cache = None
        self._missing_ref = {}

    def check_row(self, cells):
        log.debug("Checking foreign keys")
        # Prepare cache
        if self.__foreign_fields_cache is None:
            self.__foreign_fields_cache = \
                ForeignKeyCheck._create_foreign_fields_cache(cells)
            log.debug("The Foreign Fields Cache:")
            log.debug(self.__foreign_fields_cache)

        # Step through the cells and check values are valid
        errors = []
        for cell in cells:

            default_field = namedtuple('field', 'descriptor')
            field = cell.get('field', default_field(descriptor={}))

            if field.descriptor.get('foreignKey'):

                if not self.__foreign_fields_cache.get(cell['header']):
                    self.__foreign_fields_cache = merge_two_dicts(
                        self.__foreign_fields_cache,
                        ForeignKeyCheck._create_foreign_fields_cache([cell])
                    )

                cell_cache = self.__foreign_fields_cache.get(cell['header'])
                valid_values = cell_cache.get('values', [])
                resource_id = cell_cache.get('resource_id', "")
                resource_url = cell_cache.get('resource_url', "")

                # Check if ref resource missing, so we only return one error
                missing = self._missing_ref.get(field.descriptor.get('name'))

                if not valid_values and cell['row-number'] <= 2:
                    errors.append(Error(
                        'foreign-key',
                        cell,
                        row_number=cell['row-number'],
                        message=("No foreign-key reference found. "
                                 "Does the referenced resource exist?")
                    ))
                    self._missing_ref[field.descriptor.get('name')] = True

                elif str(cell['value']) not in valid_values and not missing:
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

        log.debug("Creating foreign key cache")
        cache = {}

        for column_number, cell in enumerate(cells, start=1):

            log.debug("Cell: {}".format(cell))
            default_field = namedtuple('field', 'descriptor')
            field = cell.get('field', default_field(descriptor={}))
            foreign_key = field.descriptor.get('foreignKey')

            if foreign_key:

                # Default to empty values if resource not found/specfied
                res_id = ""
                values = []

                # If field is a string, then it is a resource reference
                if isinstance(foreign_key, basestring):
                    try:
                        res_id, field = tuple(foreign_key.split(':')[0:2])
                        log.debug("Getting valid foreign key values")
                        values = ForeignKeyCheck._get_valid_values(
                            res_id,
                            field
                        )
                    except t.ObjectNotFound:
                        pass

                # If field is a list, then the valid values already determined
                elif type(foreign_key) is list:
                    res_id = ""
                    values = foreign_key

                cache[cell['header']] = {
                    'values': values,
                    'resource_id': res_id
                }

        log.debug("Foreign Key cache: {}".format(cache))
        return cache

    @staticmethod
    def _get_valid_values(resource_id, field):
        data_dict = {
            'resource_id': resource_id,
            'fields': [field],
            'limit': 3000
        }
        register_translator()
        result = t.get_action('datastore_search')(
            # FIXME: Should we really ignore the auth here?
            {'ignore_auth': True},
            data_dict
        )
        log.debug("Got valid values: {}".format(result))
        valid_values = [str(x[field]) for x in result.get('records', [])]
        return valid_values


def register_translator():
    # Workaround until core translation function defaults to Flask
    from paste.registry import Registry
    from ckan.lib.cli import MockTranslator
    registry = Registry()
    registry.prepare()
    from pylons import translator
    registry.register(translator, MockTranslator())


def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z
