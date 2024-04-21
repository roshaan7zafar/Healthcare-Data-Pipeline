from datetime import datetime

# CONSTANTS
# BUCKET_NAME = "eventus-xml" #"pdh-xml-bucket"
# XML_PREFIX_NAME = "Batch2/"
# PDFS_PREFIX_NAME = "Batch2PDFs-fixed/"
# PER_PAGE = 1000

BUCKET_NAME = "pdh-xml-buckets-mumbai" #"pdh-xml-bucket"
XML_PREFIX_NAME = "testbatch-xmls/"
PDFS_PREFIX_NAME = "testbatch-pdfs/"
PER_PAGE = 1000

# WKHTMLTOPDF_PATH = "C:\\Program Files\\wkhtmltopdf\\bin\\wkhtmltopdf.exe"
WKHTMLTOPDF_PATH = "/usr/bin/wkhtmltopdf"


def format_date(date_str , get_age=True):
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])
    
    month_name = datetime(year, month, day).strftime('%B')
    
    current_year = datetime.now().year
    if get_age:
        age = current_year - year
        return f"{month_name} {day}, {year} ({age}yr)"
    else:
        return f"{month_name} {day}, {year}"
    
def formate_time(input_time):
    # str -> datetime obj
    dt = datetime.strptime(input_time, "%Y%m%d%H%M%S%z")
    # datetime -> str formatted
    formatted_date = dt.strftime("%B %d, %Y, %I:%M:%S%p %z")
    
    return formatted_date

def add_field(prefix , field_name , value):
    return f"""
            <td class="td_label td_label_width">{field_name}</td><td>{prefix} {value}</td>
    """

def get_default_namespace(root):
    if root.tag.startswith("{"):
        ns, tag = root.tag.split("}", 1)
        return ns + "}" 
    return None

# get tag attribute value
def get_values(root , path , attributes):
    obj = root.find(path)
    value = ""
    if len(attributes) > 1:
        for a in attributes:
            value_to_add = obj.attrib.get(a)
            if value_to_add is not None:
                value += (value_to_add + " ")
    else:
        value_to_add = obj.attrib.get(attributes[0])
        if value_to_add is not None:
            value += (value_to_add)

    return value

def return_contacts(root , path , ns , contact_elems_tuple=None):
    # CDA address code: Prefix
    addresses_string = ""
    tele_string = ""

    contacts_mappings= {
        "HP": "Home Primary",
        "H": "Home",
        "WP": "Workplace",
        "MC": "Mobile"
    }

    if contact_elems_tuple:
        addresses , telecoms = contact_elems_tuple
    else:
        addresses = root.findall(f"{path}/{ns}addr")
        telecoms = root.findall(f"{path}/{ns}telecom")

    for addr in addresses:
        prefix = addr.attrib.get("use", "")
        prefix = contacts_mappings.get(prefix, prefix)
        if prefix:
            prefix += ": "

        address_str = prefix
        for tag in addr:
            if tag.text:
                address_str += (tag.text + " ")

        addresses_string+= address_str  
        addresses_string+= "<br>"

    tele_string = "" 
    for tele in telecoms:
        prefix = tele.attrib.get("use", "")
        prefix = contacts_mappings.get(prefix, prefix)
        if prefix:
            prefix += ": "
        
        tele_string += prefix
        tele_string += tele.attrib.get("value", "")
        tele_string += "<br>"


    return addresses_string + tele_string

def return_full_name(root , path , ns , xml_element=None):
    if xml_element:
        elem = xml_element
    else:
        elem = root.find(f"{path}/{ns}name")
    full_name = ""
    if len(elem) > 1:
        for tag in elem:
            full_name+= (tag.text + " ")
    else:
        full_name+= elem.text

    return full_name



# GET/EXTRACT XML UTIL FUNCTIONS
def get_document_field(paths , root, *attributes):
    path = paths.get('Document')[0]
    document = get_values(root, path, attributes)
    return document

def get_created_at_field(paths , root , *attributes):
    path = paths.get('created_on')[0]
    created_at = get_values(root, path, attributes)
    created_at = formate_time(created_at)
    return created_at

def get_custodian(paths , root , ns , unique_providergroup_id):
    path = paths.get('custodian')[0]
    path = f"{path}/{ns}assignedCustodian/{ns}representedCustodianOrganization"
    custodian = return_full_name(root , path , ns)
    custodian = custodian + ", " + unique_providergroup_id
    return custodian

def get_custodian_contacts(paths , root , ns):
    path = paths.get('custodian_contacts')[0]
    custodian_contacts = return_contacts(root , path , ns)
    return custodian_contacts

def get_patient_name(paths , root , ns):
    path = paths.get('patient')[0]
    patient = return_full_name(root  , path , ns)
    return patient

def get_patient_contacts(paths , root , ns):
    path = paths.get('patient_role')[0]
    patient_contacts = return_contacts(root , path , ns)
    return patient_contacts

def get_patient_birthtime(paths , root , ns  ,*attributes ):
    path = paths.get('patient')[0]
    path = f"{path}/{ns}birthTime"
    date_of_birth = get_values(root, path, attributes)
    date_of_birth = format_date(date_of_birth)
    return date_of_birth

def get_patient_gender(paths , root , ns , *attributes):
    path = paths.get('patient')[0]
    path = f"{path}/{ns}administrativeGenderCode"
    gender = get_values(root, path, attributes)
    return gender

def get_patient_race(paths , root , ns , *attributes): 
    path = paths.get('patient')[0]
    path = f"{path}/{ns}raceCode"
    race = get_values(root, path, attributes)
    return race

def get_patient_ethnicity(paths , root , ns  , *attributes):
    path = paths.get('patient')[0]
    path = f"{path}/{ns}ethnicGroupCode"
    ethnicity = get_values(root, path, attributes)
    return ethnicity

def get_patient_ids(paths , root , ns):
    path = paths.get('patient_role')[0]
    patient_ids = ""
    ids = root.findall(f"{path}/{ns}id")
    for i in range(len(ids)):
        patient_ids+= ids[i].attrib.get("extension" , "")
        patient_ids+="<br>"
        patient_ids+= ids[i].attrib.get("root" , "")
    
    return patient_ids

def get_language(paths , root , ns  , *attributes):
    path = paths.get('patient')[0]
    path = f"{path}/{ns}languageCommunication/{ns}languageCode"
    language_communication = get_values(root, path, attributes)
    return language_communication

def get_provider_organization(paths , root , ns , *attributes):
    path = paths.get('provider_organization')[0]
    provider_organization = return_full_name(root , path , ns)
    path = f"{path}/{ns}id"
    provider_organization+="<br>"
    provider_organization += get_values(root , path , attributes)
    return provider_organization

def get_provider_organization_contacts(paths , root , ns):
    path = paths.get('provider_organization')[0]
    provider_organization_contacts = return_contacts(root , path , ns)
    return provider_organization_contacts

def get_documentation_of_care_provision(paths , root , ns  , *attributes):
    path = paths.get('documentationOf')[0]
    low = format_date(get_values(root ,f"{path}/{ns}serviceEvent/{ns}effectiveTime/{ns}low" , attributes) , False)
    high = format_date(get_values(root ,f"{path}/{ns}serviceEvent/{ns}effectiveTime/{ns}high" , attributes) , False)
    documentation_of_care_provision = f"from {low} to {high}"
    return documentation_of_care_provision

# author is passed for filtering performers == author
def get_performers(paths , root , ns , author):
    performers_names_list = []
    performers_contacts_list = []
    path = paths.get('documentationOf')[0]
    performers_path = f"{path}/{ns}serviceEvent/{ns}performer"
    performers = root.findall(performers_path)
    for performer in performers:
        # html+="<tr>"

        assigned_person_elem = performer.find(f"{ns}assignedEntity/{ns}assignedPerson/{ns}name")
        performer_name = return_full_name(root , "" , ns , assigned_person_elem)
        # html+= add_field("" , "Performer" , performer_name)
        performers_names_list.append(performer_name)

        # performer contacts
        assigned_entity_elem = performer.find(f"{ns}assignedEntity")
        addresses_elems = assigned_entity_elem.findall(f"{ns}addr")
        telecom_elems = assigned_person_elem.findall(f"{ns}telecom")
        contacts_elems = (addresses_elems , telecom_elems)
        performer_contacts = return_contacts(root , "" , ns , contacts_elems)
        performers_contacts_list.append(performer_contacts)
        # html+= add_field("" , "Contacts" , performer_contacts)

        # html+="</tr>"
    if performers_names_list is None:
        performers_names_list = []
    if performers_contacts_list is None:
        performers_contacts_list = []

    # filter performer == author
    if author in performers_names_list:
        return (author , performers_names_list , performers_contacts_list)

    return (None , performers_names_list , performers_contacts_list)

def get_author_name(paths , root , ns):
    path = paths.get("author")[0]
    author = return_full_name(root , f"{path}/{ns}assignedAuthor/{ns}assignedPerson" , ns)
    return author

def get_author_contacts(paths , root , ns):
    path = paths.get('author')[0]
    author_contacts = return_contacts(root , f"{path}/{ns}assignedAuthor" , ns)
    return author_contacts

def get_bold_span(name):
    return f"<span style='font-weight:bold;color:#003366;'>{name}: </span>"

def get_summary_patient(paths , root , ns , summary_name ,  patient , date_of_birth , gender , patient_ids):
    summary_patient = (
        patient +
        f"{get_bold_span('Date of Birth')}" + date_of_birth + " " +
        f"{get_bold_span('Gender')}" + gender + " " +
        f"{get_bold_span('Patient-ID')}" + patient_ids
    )
    return summary_patient

def get_summary_performers(performers_names_list):
    summary_performers = ""
    for performer in performers_names_list:
        summary_performers+= f"{get_bold_span('Performer')} {performer}, "
    return summary_performers

def get_summary_documentation_of(documentation_of_care_provision , performer_author):
    summary_documentation_of = (
        # f"{get_bold_span('Date/Time')}" + documentation_of_care_provision +
        # summary_performers
        performer_author
    )
    return summary_documentation_of

def get_summary_author(paths , root , ns , provider_organization , author):
    author_time_path = paths.get("author")[0]
    author_time_path = f"{author_time_path}/{ns}time"
    author_time = format_date((root.find(author_time_path)).attrib.get('value') , False)
    summary_author = (
        f"{author}, {get_bold_span('Organization')}: {provider_organization}, {get_bold_span('Authored On')}: {author_time}"
    )
    return summary_author








































# def add_first_section(root):
#     global html
#     html+= """
#     <table class="header_table">
#     <tbody>
#     """

#     # GET DOCUMENT ATTRIBUTE

#     clinical_id_extension = None
#     clinical_id_root = None
#     for x in root:
#         if x.tag.rsplit('}')[-1] == 'id':
#             clinical_id_extension = x.attrib.get('extension')
#             clinical_id_root = x.attrib.get('root')
#             value = clinical_id_extension + " " +  clinical_id_root
#             break
#     prefix = "ID:"
#     html+= add_row(prefix , "Document" , value)
    
#     # GET CREATED_ON ATTRIBUTE


#     html+= "</tbody>"

    # paths = {
    #     "Document": ("ID" , f"{default_namespace}id" , False , "extension" , "root")
    # }