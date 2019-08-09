# encoding: utf-8
import cStringIO
import pandas
import logging
import datetime
import json
import re
import os
import requests
import tabulator
import math
from sqlalchemy.orm.exc import NoResultFound
from goodtables import validate
from ckan.model import Session
import ckan.lib.uploader as uploader
from schemed_table import SchemedTable
import ckantoolkit as t

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
        {'ignore_auth': True}, {'id': resource['package_id']})

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

    schema = resource.get(u'schema')
    if schema and isinstance(schema, basestring):
        if schema.startswith('http'):
            r = requests.get(schema)
            schema = r.json()
        elif schema[0].strip() not in ['{', '[']:  # If not a valid json string
            schema_directory = t.config['ckanext.validation.schema_directory']
            file_path = schema_directory + '/' + schema.strip() + '.json'
            schema = load_schema_from_path(file_path).schema
        else:
            schema = json.loads(schema)

    file_format = resource.get(u'format', u'').lower()
    df = _load_dataframe(source, file_format)
    actual_headers = df.columns
    if schema.get("transpose"):
        transposed = _transpose_dataframe(df)
        source = _dump_dataframe(transposed, file_format, source)
    else:
        source = _dump_dataframe(df, file_format, source)

    _format = resource[u'format'].lower()
    logging.warning(options)
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
            x['row'] = list(df.iloc[x['row-number']-1].fillna(''))
        except IndexError:
            x['row'] = []
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


def load_schema_from_path(path):
    """
    Given an absolute file path (beginning with /) load a json schema object
    in that file.
    """
    if os.path.exists(path):
        try:
            return SchemedTable(path)
        except Exception:
            log.error("Error reading schema " + path)
            raise
    else:
        raise IOError(path + " file not found")
