import json
import pandas as pd
from collections import OrderedDict
from io import open


class SchemedTable:
    """
    A wrapper class for a Frictionless Data table schema.  These schemas are
    used by our validator extension to check the data upon upload.
    """

    def __init__(self, fpath):
        self.fname = fpath.split('/')[-1][:-5]  # fName w/o path or extension
        with open(fpath, encoding='utf-8') as read_file:
            self.schema = json.load(read_file)

    def create_template(self):
        """
        Creates a template dataframe from the JSON schema.
        """

        # Reindex foreign key information so it is more easily accessible
        data = OrderedDict()
        foreign_keys = {v['fields']: v for v in self.schema.get('foreignKeys', [])}

        # Create the csv template column by column
        for f in self.schema['fields']:

            # Pop enum out of constraints as we treat this seperately
            enum = f.get('constraints', {}).pop('enum', [])

            # State basic field constraints
            default_values = ["", "--conditions--", "type: "+str(f['type'])]
            for k, v in f.get('constraints', {}).iteritems():
                default_values += [str(k)+": "+str(v)]

            # List possible values if enum field
            if enum:
                default_values += ["", "--restricted values--"] + enum

            # Detail any foreign references
            if f['name'] in foreign_keys.keys():
                default_values += [
                    "",
                    "--foreign key--",
                    "field: " + str(foreign_keys[f['name']]['reference']['fields']),
                    "resource: " + str(foreign_keys[f['name']]['reference']['resource']),
                ]

            # Specify whether the field contributes to a key of some sort
            if f['name'] == self.schema.get('primaryKey', ""):
                default_values += ["", "--primary key--"]
            elif f['name'] in self.schema.get('primaryKey', []):
                default_values += ["", "--composite key--"]

            # Insert sample data, or else the conditions assembled above
            data[f['name']] = [f['name']]+map(str, f.get(
                'example_values',
                default_values
            ))

        template = pd.DataFrame.from_dict(data, orient='index').transpose()

        # Transpose the data if the schema says so
        if self.schema.get('transpose'):
            template = template.transpose()

        return template

    def create_csv_template(self, fname=None, directory="."):
        """
        Creates a csv template from the GoodTables schema.
        """
        if fname is None:
            fname = self.fname + "_template.csv"
        template = self.create_template()
        file = open(directory+"/"+fname, "w")
        csv_table = template.to_csv(header=True, index=False, encoding='utf-8')
        file.write(unicode(csv_table, encoding='utf-8'))
        file.close()

    def create_table(self):
        """
        This function should be overriden by sub-classes. It should create
        a table from the schema and populate it with data from a Spectrum File.
        """
        return self.create_template()

    def create_csv_table(self, spectrum_file, fname=None):
        """
        Creates a csv table from the GoodTables schema.
        """
        if fname is None:
            fname = self.fname + "_" + spectrum_file.country + ".csv"
        table = self.create_table(spectrum_file)
        file = open(fname, "w")
        csv_table = table.to_csv(header=True, index=False, encoding='utf-8')
        file.write(unicode(csv_table, encoding='utf-8'))
        file.close()
