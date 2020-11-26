# encoding: utf-8

import pytest
import mock
from ckan.lib.helpers import url_for
import logging
import ckan.model as model
from ckanext.validation.model import create_tables, tables_exist
from ckanext.validation.jobs import run_validation_job
from ckanext.validation.helpers import show_validation_schemas
import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

PLUGIN_CONTROLLER = 'ckanext.ytp.request.controller:YtpRequestController'


@pytest.fixture
def initdb():
    if not tables_exist():
        create_tables()


@pytest.mark.usefixtures(u'initdb')
@pytest.mark.usefixtures(u'clean_db')
@pytest.mark.ckan_config(u'ckan.plugins', u'validation scheming_datasets unaids')
@pytest.mark.ckan_config(u'ckanext.validation.run_on_create_async', 'True')
@pytest.mark.usefixtures(u'with_plugins')
class TestAsyncValidationTriggers(object):
    '''
        Test that viewing an already actioned membership request does not 404
    '''

    @mock.patch('ckanext.validation.logic.enqueue_job')
    def test_create_resource_for_validation(self, mock_enqueue):
        sysadmin = factories.Sysadmin()
        dataset = factories.Dataset(type='dataset-test')
        resource = {'format': 'csv', 'url_type': 'upload', 'schema': 'test_schema', 'package_id': dataset['id']}
        helpers.call_action('resource_create', None, **resource)
        assert mock_enqueue.call_count == 1
        assert mock_enqueue.call_args[0][0] == run_validation_job
        assert mock_enqueue.call_args[0][1][0]['id'] == resource['id']

    def test_update_resource_for_validation(self):
        pass

    def test_create_resource_not_for_validation(self):
        pass

    def test_update_resource_not_for_validation(self):
        pass

    def test_create_resource_for_validate_package(self):
        pass

    def test_update_resource_for_validate_package(self):
        pass
