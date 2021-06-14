from ckanext.validation.model import tables_exist, create_tables


def validation_db_setup():
    if not tables_exist():
        create_tables()
