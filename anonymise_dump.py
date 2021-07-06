from faker import Faker
fake = Faker()

process_parties_party = False
parties_party_fields = ["created_at", "updated_at", "id", "name", "address", "website", "type", "sub_type", "role",
                        "copy_of_id", "country_id", "organisation_id", "clearance_level", "descriptors", "details",
                        "email", "phone_number", "role_other", "sub_type_other", "signatory_name_euu"]


def process_line(line):
    global process_parties_party
    if "COPY public.parties_party " in line:
        process_parties_party = True
        return line
    if line == "\\.\n":
        process_parties_party = False
        return line
    if process_parties_party:
        cols = line.split("\t")
        cols[parties_party_fields.index("name")] = fake.name()
        cols[parties_party_fields.index("address")] = fake.address().replace("\n",", ")
        cols[parties_party_fields.index("website")] = fake.domain_name(2)
        cols[parties_party_fields.index("email")] = fake.company_email()
        cols[parties_party_fields.index("phone_number")] = fake.phone_number()
        return "\t".join(cols)
    return line


with open("sample_outfile.sql") as dump_f:
    with open("anonymised.sql", "w") as anon_f:
        line = dump_f.readline()
        while line:
            new_line = process_line(line)
            anon_f.write(new_line)
            line = dump_f.readline()
