# encoding: utf-8
import cStringIO
import pandas
import logging
import datetime
import json
import re
import requests
from sqlalchemy.orm.exc import NoResultFound
from goodtables import validate
from ckan.model import Session
import ckan.lib.uploader as uploader
import ckantoolkit as t
from helpers import validation_load_json_schema

from ckanext.validation.model import Validation


log = logging.getLogger(__name__)


def run_validation_job(resource):

    log.debug(u'Validating resource {}'.format(resource['id']))

    try:
        validation = Session.query(Validation).filter(
            Validation.resource_id == resource['id']).one()
    except NoResultFound:
        validation = None

    if not validation:
        validation = Validation(resource_id=resource['id'])

    validation.status = u'running'
    Session.add(validation)
    Session.commit()

    options = t.config.get(
        u'ckanext.validation.default_validation_options')
    if options:
        options = json.loads(options)
    else:
        options = {}

    resource_options = resource.get(u'validation_options')
    if resource_options and isinstance(resource_options, basestring):
        resource_options = json.loads(resource_options)
    if resource_options:
        options.update(resource_options)

    dataset = t.get_action('package_show')(
        {'ignore_auth': True},
        {'id': resource['package_id']}
    )

    source = None
    if resource.get(u'url_type') == u'upload':
        upload = uploader.get_resource_uploader(resource)
        if isinstance(upload, uploader.ResourceUpload):
            source = upload.get_path(resource[u'id'])
        else:
            # Upload is not the default implementation (ie it's a cloud storage
            # implementation)
            pass_auth_header = t.asbool(
                t.config.get(u'ckanext.validation.pass_auth_header', True))
            if dataset[u'private'] and pass_auth_header:
                s = requests.Session()
                s.headers.update({
                    u'Authorization': t.config.get(
                        u'ckanext.validation.pass_auth_header_value',
                        _get_site_user_api_key())
                })

                options[u'http_session'] = s

    if not source:
        source = resource[u'url']

    # Load the the schema as a dictionary
    schema = resource.get(u'schema')
    if schema and isinstance(schema, basestring):
        schema = validation_load_json_schema(schema)

    # Load the data as a dataframe
    _format = resource.get(u'format', u'').lower()
    original_df = _load_dataframe(source, _format)
    actual_headers = original_df.columns

    # Some of the tables (lists of indicators) are transposed for readability
    altered_df = original_df.copy()
    if schema.get("transpose"):
        altered_df = _transpose_dataframe(original_df)

    # Foreign keys requires using resource metadata in validation step
    # We insert the metadata into schema here
    _prep_foreign_keys(dataset, schema, resource, altered_df)

    # Having extracted/altered data, we write back to disk for validation.
    source = _dump_dataframe(altered_df, _format, source)
    report = _validate_table(source, _format=_format, schema=schema, **options)

    # Hide uploaded files
    for table in report.get('tables', []):
        if table['source'].startswith('/'):
            table['source'] = resource['url']
    for index, warning in enumerate(report.get('warnings', [])):
        report['warnings'][index] = re.sub(r'Table ".*"', 'Table', warning)

    # If table was transposed for validation, reverse the transposition
    if schema.get("transpose"):
        report_string = json.dumps(report)
        report_string = re.sub(r'(column)(-| )', r'x987asdwn23l\2', report_string)
        report_string = re.sub(r'(Column)(-| )', r'x987asdwn23u\2', report_string)
        report_string = re.sub(r'(row)(-| )', r'column\2', report_string)
        report_string = re.sub(r'(Row)(-| )', r'Column\2', report_string)
        report_string = re.sub(r'x987asdwn23l', 'row', report_string)
        report_string = re.sub(r'x987asdwn23u', 'Row', report_string)
        report = json.loads(report_string)
        report['tables'][0]['headers'] = list(actual_headers)

    # FIXME: Not clear on why I have to add the row back in to the report here
    def get_row(x):
        try:
            x['row'] = list(original_df.iloc[x['row-number']-1].fillna(''))
        except (IndexError, KeyError):
            x['row'] = list(original_df.iloc[0].fillna(''))
            x['row-number'] = 1
        return x

    report['tables'][0]['errors'] = list(map(
        get_row,
        report['tables'][0]['errors']
    ))

    if report['table-count'] > 0:
        validation.status = u'success' if report[u'valid'] else u'failure'
        validation.report = report
    else:
        validation.status = u'error'
        validation.error = {
            'message': '\n'.join(report['warnings']) or u'No tables found'}
    validation.finished = datetime.datetime.utcnow()

    Session.add(validation)
    Session.commit()

    # Store result status in resource
    t.get_action('resource_patch')(
        {'ignore_auth': True,
         'user': t.get_action('get_site_user')({'ignore_auth': True})['name'],
         '_validation_performed': True},
        {'id': resource['id'],
         'validation_status': validation.status,
         'validation_timestamp': validation.finished.isoformat()})


def _load_dataframe(data, extension):
    # Read in table
    if extension == "csv":
        df = pandas.read_csv(open(data, 'rU'), header=None, index_col=None)
    elif extension in ["xls", "xlsx"]:
        df = pandas.read_excel(open(data, 'rU'), header=None, index_col=None)
    df.columns = df.iloc[0]
    df.index = df[df.columns[0]]
    return df


def _transpose_dataframe(df):
    # Transpose table
    if len(df.columns) == 0:
        transposed = df
    else:
        transposed = df.T
    return transposed


def _dump_dataframe(df, extension, filepath):
    df = df.iloc[1:]
    # Write out table
    if extension == "csv":
        # HACK - For some reason can't get cStringIO to work with CSV files.
        # goodtables compalins that there is no object "readable"
        out = filepath+".tmp"
        df.to_csv(out, columns=None, index=None)
    elif extension in ["xls", "xlsx"]:
        out = cStringIO.StringIO()
        df.to_excel(out, columns=None, index=None)
        out.seek(0)
    return out


def _validate_table(source, _format=u'csv', schema=None, **options):
    report = validate(
        source,
        format=_format,
        schema=schema,
        **options
    )
    log.debug(u'Validating source: {}'.format(source))
    return report


def _get_site_user_api_key():

    site_user_name = t.get_action('get_site_user')({'ignore_auth': True}, {})
    site_user = t.get_action('get_site_user')(
        {'ignore_auth': True}, {'id': site_user_name})
    return site_user['apikey']


def _prep_foreign_keys(package, table_schema, resource, df):

    foreign_keys = {}

    for key in table_schema['foreignKeys']:

        resources = {v['schema']: v for v in package['resources']}
        field = key['fields']
        reference = key['reference']['resource']
        form_field = 'foreign-key-' + field,

        # An empty reference indicates another field in the same table
        # Far easier to get valid values from the table and insert here.
        if reference == "":
            foreign_keys[field] = list(df[key['reference']['fields']][1:])
        # Fields in resource of form "foreign-key-<field>" store references
        # Insert these user-specified references into the schema
        elif form_field in resource.keys():
            foreign_keys[field] = resource[form_field] + ":" + field
        # If no reference in form, check if reference is in same package
        elif reference in resources.keys():
            foreign_keys[field] = resources[reference]['id'] + ":" + field
        # Default to some unique value identifying a reference not found error.
        else:
            foreign_keys[field] = "NOTFOUND:" + field

    for field in table_schema['fields']:
        if field['name'] in foreign_keys.keys():
            field['foreignKey'] = foreign_keys[field['name']]
