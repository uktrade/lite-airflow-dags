import os

from faker import Faker

fake = Faker()
import yaml

import pathlib
script_dir = pathlib.Path(__file__).parent.resolve()

with open(os.path.join(script_dir, "anonymise_this.yaml")) as f:
    tables_to_anon = yaml.load(f, Loader=yaml.BaseLoader)


class Anonymiser:
    def __init__(self):
        self.current_table = None

    def process_line(self, line):
        if "COPY public." in line:
            table_name, columns = line.replace("COPY public.", "").split(" (")
            if table_name in tables_to_anon:
                columns = columns.split(")")[0].split(", ")
                self.current_table = (table_name, columns)
                return line
        if line == "\\.\n":
            self.current_table = None
            return line
        if self.current_table:
            cols = line.split("\t")
            columns_to_anon = tables_to_anon[self.current_table[0]]
            fields = self.current_table[1]
            for column_to_anon in columns_to_anon:
                value = "fake"
                if column_to_anon == "name":
                    value = fake.name()
                elif column_to_anon == "first_name":
                    value = fake.first_name()
                elif column_to_anon == "last_name":
                    value = fake.last_name()
                elif column_to_anon == "address":
                    value = fake.address().replace("\n", ", ")
                elif column_to_anon == "website":
                    value = fake.domain_name(2)
                elif column_to_anon == "email":
                    value = fake.company_email()
                elif column_to_anon == "phone_number":
                    value = fake.phone_number()
                index_to_change = fields.index(column_to_anon)
                if index_to_change == len(cols) - 1:
                    cols[fields.index(column_to_anon)] = value + "\n"
                else:
                    cols[fields.index(column_to_anon)] = value
            return "\t".join(cols)
        return line

    def anonymise(self):
        with open("outfile.sql") as dump_f:
            with open("anonymised.sql", "w") as anon_f:
                line = dump_f.readline()
                while line:
                    new_line = self.process_line(line)
                    anon_f.write(new_line)
                    line = dump_f.readline()
