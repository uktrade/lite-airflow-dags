import os
import pathlib

import yaml
from faker import Faker

fake = Faker()


class DBAnonymiser:
    def __init__(self):
        self.current_table = None
        script_dir = pathlib.Path(__file__).parent.resolve()
        with open(os.path.join(script_dir, "anonymise_model_config.yaml")) as f:
            self.tables_to_anon = yaml.load(f, Loader=yaml.BaseLoader)

    def process_line(self, line):
        if "COPY public." in line:
            table_name, columns = line.replace("COPY public.", "").split(" (")
            if table_name in self.tables_to_anon:
                columns = columns.split(")")[0].split(", ")
                self.current_table = (table_name, columns)
                return line
        if line == "\\.\n":
            self.current_table = None
            return line
        if self.current_table:
            cols = line.split("\t")
            columns_to_anon = self.tables_to_anon[self.current_table[0]]
            fields = self.current_table[1]
            for column_to_anon in columns_to_anon:
                value = "fake"
                if column_to_anon == "name" and self.current_table[0] == "organisation":
                    value = fake.company()
                elif column_to_anon in [
                    "name",
                    "signatory_name_euu",
                    "consignee_name",
                    "contact_name",
                ]:
                    value = fake.name()
                elif column_to_anon == "first_name":
                    value = fake.first_name()
                elif column_to_anon == "last_name":
                    value = fake.last_name()
                elif column_to_anon == "address":
                    value = fake.address().replace("\n", ", ")
                elif column_to_anon == "address_line_1":
                    value = fake.street_address()
                elif column_to_anon in ["address_line_2", "region", "city"]:
                    value = fake.city()
                elif column_to_anon == "postcode":
                    value = fake.postcode()
                elif column_to_anon == "website":
                    value = fake.domain_name(2)
                elif column_to_anon in ["email", "contact_email"]:
                    value = fake.company_email()
                elif column_to_anon in ["phone_number", "contact_telephone"]:
                    value = fake.phone_number()
                elif column_to_anon == "details":
                    value = fake.paragraph(nb_sentences=5)
                index_to_change = fields.index(column_to_anon)
                if index_to_change == len(cols) - 1:
                    cols[fields.index(column_to_anon)] = value + "\n"
                else:
                    cols[fields.index(column_to_anon)] = value
            return "\t".join(cols)
        return line

    def anonymise(self, outfile="outfile.sql", anonfile="anonymised.sql"):
        with open(outfile) as dump_f, open(anonfile, "w") as anon_f:
            line = dump_f.readline()
            while line:
                new_line = self.process_line(line)
                anon_f.write(new_line)
                line = dump_f.readline()
