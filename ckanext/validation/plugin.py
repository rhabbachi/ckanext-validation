# encoding: utf-8

import logging
import cgi
import json
import os
import ckan.plugins as p
from ckan.common import _
import ckantoolkit as t
from custom_checks import (
    ForeignKeyCheck,
    geometry_check,
    UniqueConstraint,
    enumerable_constraint,
    register_translator
)
from goodtables.registry import registry, spec
from ckanext.validation import settings
from ckanext.validation.model import tables_exist
from ckanext.validation.logic import (
    resource_validation_run, resource_validation_show,
    resource_validation_delete, resource_validation_run_batch,
    auth_resource_validation_run, auth_resource_validation_show,
    auth_resource_validation_delete, auth_resource_validation_run_batch,
    resource_create as custom_resource_create,
    resource_update as custom_resource_update,
)
from ckanext.validation.helpers import (
    get_validation_badge,
    validation_extract_report_from_errors,
    dump_json_value,
    bootstrap_version,
    show_validation_schemas,
    validation_get_foreign_keys,
    validation_get_goodtables_spec
)
from ckanext.validation.validators import (
    resource_schema_validator,
    validation_options_validator,
)
from ckanext.validation.utils import (
    get_create_mode_from_config,
    get_update_mode_from_config,
)
from ckan.lib.plugins import DefaultTranslation

log = logging.getLogger(__name__)


class ValidationPlugin(p.SingletonPlugin, DefaultTranslation):

    p.implements(p.IConfigurable)
    p.implements(p.IConfigurer)
    p.implements(p.IActions)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IAuthFunctions)
    p.implements(p.IResourceController, inherit=True)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IValidators)
    p.implements(p.ITranslation)

    # IConfigurable
    def configure(self, config):
        # Check that if schema_directory given, it exists.
        schema_directory = config.get('ckanext.validation.schema_directory')
        if schema_directory:
            if not os.path.isdir(schema_directory):
                raise ValueError(
                    'ckanext.validation.schema_directory not a valid path'
                )

    # IConfigurer
    def update_config(self, config_):
        if not tables_exist():
            log.critical(u'''
The validation extension requires a database setup. Please run the following
to create the database tables:
    paster --plugin=ckanext-validation validation init-db
''')
        else:
            log.debug(u'Validation tables exist')

        t.add_template_directory(config_, u'templates')
        t.add_public_directory(config_, u'public')
        t.add_resource(u'fanstatic', 'ckanext-validation')

    # IRoutes

    def before_map(self, map_):

        controller = u'ckanext.validation.controller:ValidationController'

        map_.connect(
            u'validation_read',
            u'/dataset/{id}/resource/{resource_id}/validation',
            controller=controller, action=u'validation')

        return map_

    # IActions

    def get_actions(self):
        new_actions = {
            u'resource_validation_run': resource_validation_run,
            u'resource_validation_show': resource_validation_show,
            u'resource_validation_delete': resource_validation_delete,
            u'resource_validation_run_batch': resource_validation_run_batch,
        }

        if get_create_mode_from_config() == u'sync':
            new_actions[u'resource_create'] = custom_resource_create
        if get_update_mode_from_config() == u'sync':
            new_actions[u'resource_update'] = custom_resource_update

        return new_actions

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            u'resource_validation_run': auth_resource_validation_run,
            u'resource_validation_show': auth_resource_validation_show,
            u'resource_validation_delete': auth_resource_validation_delete,
            u'resource_validation_run_batch': auth_resource_validation_run_batch
        }

    # ITemplateHelpers
    def get_helpers(self):
        return {
            u'get_validation_badge': get_validation_badge,
            u'validation_extract_report_from_errors': validation_extract_report_from_errors,
            u'dump_json_value': dump_json_value,
            u'bootstrap_version': bootstrap_version,
            u'validator_show_validation_schemas': show_validation_schemas,
            u'validation_get_foreign_keys': validation_get_foreign_keys,
            u'validation_get_goodtables_spec': validation_get_goodtables_spec
        }

    # IResourceController
    def _process_schema_fields(self, data_dict):
        u'''
        Normalize the different ways of providing the `schema` field

        1. If `schema_upload` is provided and it's a valid file, the contents
           are read into `schema`.
        2. If `schema_url` is provided and looks like a valid URL, it's copied
           to `schema`
        3. If `schema_json` is provided, it's copied to `schema`.

        All the 3 `schema_*` fields are removed from the data_dict.
        Note that the data_dict still needs to pass validation
        '''

        schema_upload = data_dict.pop(u'schema_upload', None)
        schema_url = data_dict.pop(u'schema_url', None)
        schema_json = data_dict.pop(u'schema_json', None)

        if isinstance(schema_upload, cgi.FieldStorage):
            data_dict[u'schema'] = schema_upload.file.read()
        elif schema_url:
            if (not isinstance(schema_url, basestring) or
                    not schema_url.lower()[:4] == u'http'):
                raise t.ValidationError({u'schema_url': _('Must be a valid URL')})
            data_dict[u'schema'] = schema_url
        elif schema_json:
            data_dict[u'schema'] = schema_json

        return data_dict

    def before_create(self, context, data_dict):
        return self._process_schema_fields(data_dict)

    resources_to_validate = {}

    def after_create(self, context, data_dict):

        if not get_create_mode_from_config() == u'async':
            return

        if data_dict.get(u'resources'):
            # This is a dataset
            for resource in data_dict[u'resources']:
                self._handle_validation_for_resource(resource)
        else:
            # This is a resource
            self._handle_validation_for_resource(data_dict)

    def _data_dict_is_dataset(self, data_dict):
        return u'creator_user_id' in data_dict or u'owner_org' in data_dict

    def _handle_validation_for_resource(self, resource):
        needs_validation = False
        if ((
            # File uploaded
            resource.get(u'url_type') == u'upload' or
            # URL defined
            resource.get(u'url')
            ) and (
            # Make sure format is supported
            resource.get(u'format', u'').lower() in
                settings.SUPPORTED_FORMATS
                )):
            needs_validation = True

        if needs_validation:
            _run_async_validation(resource[u'id'])

    def before_update(self, context, current_resource, updated_resource):

        updated_resource = self._process_schema_fields(updated_resource)

        if not get_update_mode_from_config() == u'async':
            return updated_resource

        needs_validation = False
        if ((
            # New file uploaded
            updated_resource.get(u'upload') or
            # External URL changed
            updated_resource.get(u'url') != current_resource.get(u'url') or
            # Schema changed
            (updated_resource.get(u'schema') !=
             current_resource.get(u'schema')) or
            # Format changed
            (updated_resource.get(u'format', u'').lower() !=
             current_resource.get(u'format', u'').lower())
            ) and (
            # Make sure format is supported
            updated_resource.get(u'format', u'').lower() in
                settings.SUPPORTED_FORMATS
                )):
            needs_validation = True

        if needs_validation:
            self.resources_to_validate[updated_resource[u'id']] = True

        return updated_resource

    def after_update(self, context, data_dict):

        if not get_update_mode_from_config() == u'async':
            return

        if context.get('_validation_performed'):
            # Ugly, but needed to avoid circular loops caused by the
            # validation job calling resource_patch (which calls
            # package_update)
            del context['_validation_performed']
            return

        if data_dict.get(u'resources'):
            # This is a dataset
            for resource in data_dict[u'resources']:
                if resource[u'id'] in self.resources_to_validate:
                    # This is part of a resource_update call, it will be
                    # handled on the next `after_update` call
                    continue
                else:
                    # This is an actual package_update call, validate the
                    # resources if necessary
                    self._handle_validation_for_resource(resource)

        else:
            # This is a resource
            resource_id = data_dict[u'id']

            if resource_id in self.resources_to_validate:
                del self.resources_to_validate[resource_id]

                _run_async_validation(resource_id)

    # IPackageController

    def before_index(self, index_dict):

        res_status = []
        dataset_dict = json.loads(index_dict['validated_data_dict'])
        for resource in dataset_dict.get('resources', []):
            if resource.get('validation_status'):
                res_status.append(resource['validation_status'])

        if res_status:
            index_dict['vocab_validation_status'] = res_status

        return index_dict

    # IValidators

    def get_validators(self):
        return {
            'resource_schema_validator': resource_schema_validator,
            'validation_options_validator': validation_options_validator,
        }


def _run_async_validation(resource_id):

    try:
        t.get_action(u'resource_validation_run')(
            {u'ignore_auth': True},
            {u'resource_id': resource_id,
             u'async': True})
    except t.ValidationError as e:
        log.warning(
            u'Could not run validation for resource {}: {}'.format(
                resource_id, str(e)))
