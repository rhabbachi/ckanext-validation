# encoding: utf-8
import six
import cStringIO
from tabulator import Stream
from tableschema import Schema, exceptions
import pandas
import logging

log = logging.getLogger(__name__)


def unordered_preset(source, schema=None, **options):
    """
    Creating a preset in which you can configure whether the order of columns matters.
    """
    warnings = []
    errors = []
    tables = []

    # Ensure not a datapackage
    if isinstance(source, six.string_types):
        if source.endswith('datapackage.json'):
            errors.append('Use "datapackage" preset for Data Packages')

    # Prepare schema
    if schema is not None:

        df = pandas.read_excel(source, dtype=str)

        if not schema.get('require_field_order', True):
            log.debug("Considering whether to switch column order")
            required_field_order = list(map(
                lambda x: x['name'],
                schema.get('fields', [])
            ))
            submitted_field_order = list(df.columns)
            for f in required_field_order:
                if f in submitted_field_order:
                    submitted_field_order.remove(f)

            new_column_order = required_field_order + submitted_field_order

            log.debug("New Order: " + str(new_column_order) +
                      " Old Order: " + str(list(df.columns)))

            if not list(new_column_order) == list(df.columns):
                log.debug("Switching column order")
                df = df[new_column_order]
                warnings.append(
                    'The fields have had to be reordered to pass validation. '
                    'Column numbers in error messages may be wrong!'
                )

        log.debug(df)

        # Rewrite the DF to source
        source = cStringIO.StringIO()
        df.to_excel(source, columns=None, index=None)
        source.seek(0)

        try:
            schema = Schema(schema)
        except exceptions.TableSchemaException as error:
            errors.append(
                'Table Schema "%s" has a loading error "%s"' %
                (schema, error))


    # Add table
    if not errors:
        options.setdefault('headers', 1)
        tables.append({
            'source': str(source) if isinstance(source, six.string_types) else 'inline',
            'stream': Stream(source, **options),
            'schema': schema,
            'extra': {},
        })

    warnings = errors + warnings

    return warnings, tables
