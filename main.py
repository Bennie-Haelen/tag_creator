import os
from datetime import datetime
import numpy as np
from google.cloud import datacatalog_v1
from modules.DataplexTagManager import DataplexTagManager

# Project/environment configuration
PROJECT_ID  = "hca-sandbox"
LOCATION_ID = "us"
DATASET_ID  = "hca_p360_pot"
TABLE_ID    = "sofa_score"

# Service account credentials
service_account_file = 'credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_file

def main():
    """
    Demonstrates how to:
      1) Lookup a Data Catalog entry from a Dataplex-linked resource (BigQuery in this case).
      2) Retrieve a tag template and display its fields.
      3) Create a new tag using that template, assigning values to each field.
    """

    # 1) Construct the Dataplex-linked resource name for the BigQuery table
    entry_name = (
        f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
    )
    print(f"Dataplex (BigQuery) resource name: {entry_name}")

    # 2) Instantiate our DataplexTagManager, which handles tag operations
    manager = DataplexTagManager()

    # 3) Lookup the Data Catalog entry (the code currently calls it "list_tags", 
    #    but it's actually using lookup_entry).
    entry = manager.list_tags(entry_name)
    print(f"Data Catalog Entry Name: {entry.name}")
    print(f"Description: {entry.description}")
    print(f"Schema: {entry.schema}")

    # 4) Retrieve the tag template by name
    tag_template_name = f"projects/{PROJECT_ID}/locations/{LOCATION_ID}/tagTemplates/table_data_governance"
    template = manager.get_tag_template(name=tag_template_name)
    print(f"Tag Template: {template.name}")

    # Print out the template fields for clarity
    print("Template fields:")
    for field_id, field in template.fields.items():
        print(f" - Field ID: {field_id}, Type: {field.type}")

    # 5) Define the field values for the new tag
    #    Make sure each field in 'fields' matches a valid field_id in the template
    fields = {
        "notes": "<p>this is a set of notes</p>",
        "business_owner": "Bennie Haelen",
        "approved_by_governance": True,
        "business_description": "this is a business description",
        "data_classification": "Internal",
        "data_governor": "Bennie Haelen",
        "data_lifecyle": "Dev",        # Make sure the correct field ID is "data_lifecycle" in the template
        "has_pii": False,
        "is_encrypted": False,
        "retention_date": "1/1/2027",  # Will be parsed as a timestamp if the template field is TIMESTAMP
    }

    # 6) Create the new tag on the entry
    #    (If the field name in the template is "data_lifecycle", 
    #     ensure you match that exactly, or the field won't be recognized.)
    new_tag = manager.create_tag(entry_name, tag_template_name, fields)

    # If create_tag() returns a Tag object, you can print its name
    if new_tag:
        print(f"Created new tag: {new_tag.name}")

if __name__ == "__main__":
    main()
