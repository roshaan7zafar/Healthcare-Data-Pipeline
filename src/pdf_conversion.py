import pprint
import pdfkit
import xml.etree.ElementTree as ET
import boto3
import os
from utils import add_field , get_default_namespace , get_values , format_date , return_contacts,return_full_name , formate_time
# import constants
from utils import add_field

# get functions
from utils import (
    get_document_field,
    get_created_at_field,
    get_custodian,
    get_custodian_contacts,
    get_patient_name,
    get_patient_contacts,
    get_patient_birthtime,
    get_patient_gender,
    get_patient_race,
    get_patient_ethnicity,
    get_patient_ids,
    get_language,
    get_provider_organization,
    get_provider_organization_contacts,
    get_documentation_of_care_provision,
    get_performers,
    get_author_name,
    get_author_contacts,
    get_bold_span,
    get_summary_patient,
    get_summary_performers,
    get_summary_documentation_of,
    get_summary_author
)

# FIRST SECTION
def add_first_section(root , ns , paths , unique_providergroup_id , html):

    # GET DOCUMENT FIELD
    document = get_document_field(paths , root, "extension" , "root")
    # GET CREATED_AT FIELD
    created_at = get_created_at_field(paths , root , "value")
    # GET CUSTODIAN
    custodian = get_custodian(paths , root , ns , unique_providergroup_id)
    
    # GET CUSTODIAN CONTACTS
    custodian_contacts = get_custodian_contacts(paths , root , ns)
    # GET PATIENT NAME
    patient = get_patient_name(paths , root, ns)
    # GET PATIENT CONTACT DETAILS
    patient_contacts = get_patient_contacts(paths , root , ns)
    # GET PATIENT BIRTHTIME
    date_of_birth = get_patient_birthtime(paths , root , ns , "value")
    # GET PATIENT GENDER
    gender = get_patient_gender(paths , root , ns , "displayName")
    # GET PATIENT RACE
    race = get_patient_race(paths , root , ns , "displayName")
    # GET PATIENT ETHNICCITY
    ethnicity = get_patient_ethnicity(paths , root , ns ,  "displayName")
    # GET PATIENT IDS:
    patient_ids = get_patient_ids(paths , root , ns)
    # GET LANGUAGE
    language_communication = get_language(paths , root , ns , "code")
    # GET providerOrganization
    provider_organization = get_provider_organization(paths , root , ns , "extension" , "root")
    # GET PROVIDER ORGANIZATION CONTACTS
    provider_organization_contacts  = get_provider_organization_contacts(paths , root , ns)
    # DOCUMENTATION OF - CARE PROVISION
    documentation_of_care_provision = get_documentation_of_care_provision(paths , root , ns , "value")
    # AUTHOR NAME
    author = get_author_name(paths , root , ns)
    # GET PERFORMERS NAMES AND THEIR RESPECTIVE CONTACTS
    performer_author , performers_names_list , performers_contacts_list = get_performers(paths , root , ns , author)
    # AUTHOR CONTACTS
    author_contacts = get_author_contacts(paths , root , ns)

    # SUMMARY
    # SUMMARY PATIENT VALUE
    summary_name = (root.find(f"{ns}title")).text
    summary_patient = get_summary_patient(paths , root , ns , summary_name ,  patient , date_of_birth , gender , patient_ids)
    # SUMMARY PERFORMERS VALUE
    summary_performers = get_summary_performers(performers_names_list)
    # SUMMARY DOCUMENTATION OF VALUE
    summary_documentation_of = get_summary_documentation_of(documentation_of_care_provision , performer_author)
    # SUMMARY AUTHOR VALUE
    summary_author = get_summary_author(paths , root , ns , provider_organization , author)

    # ADDING HTML ---------------------------
    # SUMMARY
    html += '<table>'
    html+= '<tr>'
    html+= f'<h3 style="text-align:center;">{summary_name}</h3>'
    html+= add_field("" , "Patient" , summary_patient)
    html+= '</tr>'

    html+= '<tr>'
    html+= add_field("" , "Documentation Of" , summary_documentation_of)
    html+= '</tr>'

    html+= '<tr>'
    html+= add_field("" , "Electronically Signed By" , summary_author)
    html+= '</tr>'

    html+= """
      <tr>
        <td style="border: none;" width="100%" colspan="4" height="20"></td>
      </tr>
    """

    # DOCUMENT
    html+= '<tr>'
    html+= add_field("ID:" , "Document" , document)

    # GET CREATED_AT FIELD
    html+= add_field("" , "created_on" , created_at)
    html+= '</tr>'

    # GET CUSTODIAN
    html+= '<tr>'
    html+= add_field("" , "Custodian" , custodian)

    # GET CUSTODIAN CONTACTS
    html+= add_field("" , "Contact Details" , custodian_contacts)
    html+= '</tr>'

    html+= """
      <tr>
        <td style="border: none;" width="100%" colspan="4" height="20"></td>
      </tr>
    """
    # GET PATIENT NAME
    html+="<tr>"
    html+= add_field("" , "Patient" , patient)

    # GET PATIENT CONTACT DETAILS
    html+= add_field("" , "Contact Details" , patient_contacts)
    html+="</tr>"

    # GET PATIENT BIRTHTIME
    html+= "<tr>"
    html+= add_field("" , "Date Of Birth" , date_of_birth)

    # GET PATIENT GENDER
    html+= add_field("" , "Gender" , gender)
    html+="</tr>"

    # GET PATIENT RACE
    html+= "<tr>"
    html+= add_field("" , "Race" , race)

    # GET PATIENT ETHNICCITY
    html+= add_field("" , "Ethnicity" , ethnicity)
    html+="</tr>"

    # GET PATIENT IDS:
    html+="<tr>"
    html+=add_field("" , "Patiend IDS" , patient_ids)

    # GET LANGUAGE
    html+= add_field("" , "Language" , language_communication)
    html+="</tr>"

    # GET providerOrganization
    html+="<tr>"
    html+= add_field("" , "Provider Organization" , provider_organization)

    # GET PROVIDER ORGANIZATION CONTACTS
    html+= add_field("" , "Contact Details" , provider_organization_contacts)
    html+="</tr>"

    html+= """
      <tr>
        <td style="border: none;" width="100%" colspan="4" height="20"></td>
      </tr>
    """

    # DOCUMENTATION OF - CARE PROVISION
    html+="<tr>"
    html+= add_field("" , "Documentation Of - care provision" , documentation_of_care_provision)
    html+= "</tr>"

    # PERFORMERS
    for i , performer in enumerate(performers_names_list):
        html+="<tr>"

        html+= add_field("" , "performer" , performer)
        html+= add_field("" , "Contact_Details" , performers_contacts_list[i])

        html+="</tr>"


    html+= """
      <tr>
        <td style="border: none;" width="100%" colspan="4" height="20"></td>
      </tr>
    """

    # AUTHOR NAME
    html+="<tr>"
    html+= add_field("" , "Electronically Signed By" , author)

    # AUTHOR CONTACTS
    html+= add_field("" , "Contact Details" , author_contacts)
    html+="</tr>"
    html+= "</table>"

    return html


def add_paragraph(root , ns):
    html = ""
    # global html
    if len(root) == 0:
        html+= f"<p>{root.text}</p>"
        return ""
    
    if root.text and root.text.strip():
        html += f"<p>{root.text.strip()}</p>"

    for elem in root:
        tag_name = elem.tag.replace(ns, '') 
        if elem.text and elem.text.strip():
            if tag_name == "caption":
                tag_name = "h4"
            html += f"<{tag_name}>{elem.text.strip()}</{tag_name}>"
        
        if elem.tail and elem.tail.strip():
            html += f"<p>{elem.tail.strip()}</p>"

    html+= "<br>"
    return html if html else ""


# for converting table xml to html
def xml_to_html(element , ns):
    # Mapping from XML tags to HTML tags
    tag_mapping = {
        'table': 'table',
        'thead': 'thead',
        'tbody': 'tbody',
        'tr': 'tr',
        'th': 'th',
        'td': 'td',
        'content': 'span'
    }
    element.tag = element.tag.split('}')[-1]
    if element.tag in tag_mapping:
        html_elem = ET.Element(tag_mapping[element.tag])
        
        # for attr, value in element.attrib.items():
        #     html_elem.set(attr, value)
        
        # add text content
        if element.text and element.text.strip():
            html_elem.text = element.text.strip()
        
        for child in element:
            html_child = xml_to_html(child , ns)
            if html_child == None:
                print('continueing forever')
                continue
            html_elem.append(html_child)
            
            # append tail text
            if child.tail and child.tail.strip():
                tail_elem = ET.Element('div')
                tail_elem.text = child.tail.strip()
                html_elem.append(tail_elem)
                if tail_elem == None:
                    print("tail_elem" , tail_elem)
        
        return html_elem
    else:
        return None


def add_table(root , ns):
    # global html
    html_root = xml_to_html(root , ns)
    html_table = ET.tostring(html_root, encoding='unicode', method='html')
    html = html_table

    html+= "<br>"

    return html if html else ""


def add_list(root , ns):
    if len(root) == 0:
        return
    
    ul = ET.Element('ul')

    for item in root:
        li = ET.SubElement(ul , 'li')
        li.text = item.text
        
        for nested_item in item:
            if nested_item.tag == f"{ns}list":
                nested_ul = add_list(nested_item, ns)
                li.append(nested_ul)

    return ul    
    # html += ET.tostring(ul , encoding='unicode' , method="html")
    # html+= "<br>"

# This function that adds all paragraphs lists tables etc of the second section to the main html
def add_text(root , ns , file_name , html):
    # global html
    if root.tag == f"{ns}text":
        if len(root) == 0:
            html+= f'<p>{root.text}</p>'
        # looping each element in <text> tag
        for elem in root:
            if elem.tag == f"{ns}paragraph":
                html += add_paragraph(elem , ns)
            elif elem.tag == f"{ns}list":
                html += ET.tostring(add_list(elem , ns) , encoding='unicode' , method='html')
                html+= "<br>"
            elif elem.tag == f"{ns}table":
                html += add_table(elem , ns)
                pass
            else:
                print('unexpected tag found in <text> tag' ,
                 "the name of root element is:" ,
                  root ,
                   "in file:" ,
                    file_name ,
                     "the name of unexpected elem is" ,
                      elem ,
                       "The contents of elem is:" ,
                        elem.text)
                
    return html if html else ""

        
def add_second_section(root , ns , file_name , html):
    # global html
    html += "<div style='text-align:left;margin-top:20px;'></div>"

    main_component = root.find(f"{ns}component/{ns}structuredBody")
    for i , component in enumerate(main_component):
        section = component.find(f"{ns}section")

        title = section.find(f"{ns}title")

        html+= f"<h3>{title.text}</h3>"

        text_tag = section.find(f"{ns}text")
        
        html = add_text(text_tag , ns , file_name , html)    

    html+= "</div>"
    return html if html else ""
    # print(html)

def find_tags(element):
    icd10_codes = {}
    snomed_ct_codes = {}

    code_system_name = element.get('codeSystemName')
    display_name = element.get('displayName')
    code = element.get('code')

    if code_system_name == "ICD10" and display_name and code:
        icd10_codes[display_name] = code
    elif code_system_name == "SNOMED-CT" and display_name and code:
        snomed_ct_codes[display_name] = code

    for child in element:
        child_icd10_codes, child_snomed_ct_codes = find_tags(child)
        icd10_codes.update(child_icd10_codes)
        snomed_ct_codes.update(child_snomed_ct_codes)

    return icd10_codes, snomed_ct_codes

def add_third_section(root, ns , html):
    # global html
    entries = root.findall(f'.//{ns}entry')

    icd10_codes = {}
    snomed_ct_codes = {}

    for entry in entries:
        entry_icd10_codes, entry_snomed_ct_codes = find_tags(entry)
        icd10_codes.update(entry_icd10_codes)
        snomed_ct_codes.update(entry_snomed_ct_codes)

    html+= "<table>"
    html += "<h3 style='text-align:center;'>ICD10 CODES</h3>"
    for disease_name , code in icd10_codes.items():
        html += "<tr>"
        html+= add_field("" , disease_name , code)
        html+= "</tr>"
    html+= "</table>"

    html+= "<table>"
    html += "<h3 style='text-align:center;'>SNOMED-CT CODES</h3>"
    for disease_name , code in snomed_ct_codes.items():
        html += "<tr>"
        html+= add_field("" , disease_name , code)
        html+= "</tr>"
    html+= "</table>"

    return html if html else ""



def processed_html(xml_data , temp_dir, new_filename, unique_providergroup_id , html):
    file_name='NA'
    # print(xml_data)
    tree = ET.parse(xml_data)
    root = tree.getroot()

    default_namespace = get_default_namespace(root)
    if default_namespace is None:
        default_namespace = ""

    paths = {
        "Document": [f"{default_namespace}id"],
        "created_on": [f"{default_namespace}effectiveTime"],
        "custodian": [f".//{default_namespace}custodian"],
        "custodian_contacts": [f".//{default_namespace}representedCustodianOrganization"],
        "patient": [f".//{default_namespace}patient"],
        "patient_role": [f'.//{default_namespace}patientRole'],
        "provider_organization": [f'.//{default_namespace}providerOrganization'],
        "documentationOf": [f'.//{default_namespace}documentationOf'],
        "author": [f'{default_namespace}author'],

    }

    
    html = add_first_section(root , default_namespace , paths , unique_providergroup_id , html)
    html = add_second_section(root , default_namespace , file_name , html)
    # ICD10 AND SNOME CODES
    html = add_third_section(root , default_namespace , html)

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    html += """
    </body>
    </html>
    """

    # config = pdfkit.configuration(wkhtmltopdf="/usr/bin/wkhtmltopdf")
    pdfkit.from_string(html, f'{temp_dir}/{new_filename}.pdf')
    # pdfkit.from_string(html, 'asd/asdasda.pdf')
    

def main(xml_data, temp_dir, new_filename, unique_providergroup_id):
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Title Here</title>
        <style>
            body {
                font-family: Arial, Helvetica, sans-serif;
            }
            h2 {
                text-align:left;
            }
            h3 {
                text-decoration:underline;
            }
            h4 {
                text-align:center;
            }

            .td_label, td {
                padding: 4px 8px;
                line-height: 1.2;
                border: 1px solid black;
            }
            .td_label {
                background-color: LightGrey;
                color: #003366;
            }

            /* Targeting the first td */
            tr td:first-child {
            width: 10%;
            }

            /* Targeting every second td */
            tr td:nth-child(2n) {
            width: 40%;
            }
            
        </style>
    </head>
    <body>
    """
    
    processed_html(xml_data, temp_dir, new_filename, unique_providergroup_id , html)



def html_to_pdf(html_filename, pdf_filename):
    pdfkit.from_file(html_filename, pdf_filename)

    # print(html)

# main('abc/0a0542b6020bf14dfcedc3e798c4a17b212b2e28caf4821103e3a0a60a81af8b_20231103.xml',
#                 'abc', 'abcabc')

# print('done')