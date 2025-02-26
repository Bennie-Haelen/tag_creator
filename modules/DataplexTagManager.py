import numpy as np
from datetime import datetime
from google.protobuf import timestamp_pb2
from google.cloud import datacatalog_v1
from modules import string_utils

class DataplexTagManager:
    """
    A manager for creating and retrieving Dataplex (Data Catalog) tags and templates.
    """

    def __init__(self):
        """
        Initializes the Data Catalog client.
        """
        self.client = datacatalog_v1.DataCatalogClient()

    def list_tags(self, entry_name: str):
        """
        Retrieves the Data Catalog entry for the given linked resource.
        (Currently named 'list_tags', but it actually calls lookup_entry().)

        :param entry_name: A Dataplex-linked resource name, e.g.:
                          "//bigquery.googleapis.com/projects/PROJECT/datasets/DATASET/tables/TABLE"
        :return: A Data Catalog Entry object.
        """
        entry = self.client.lookup_entry(request={"linked_resource": entry_name})
        return entry

    def get_tag_template(self, name: str) -> datacatalog_v1.types.TagTemplate:
        """
        Fetches a tag template by its resource name.

        :param name: Resource name of the tag template, e.g.:
                     "projects/PROJECT_ID/locations/LOCATION_ID/tagTemplates/TEMPLATE_ID"
        :return: The TagTemplate object.
        """
        return self.client.get_tag_template(name=name)

    def create_tag(self, entry_name: str, tag_template_name: str, fields: dict) -> datacatalog_v1.types.Tag:
        """
        Creates a new tag on the specified Data Catalog entry using an existing tag template.

        :param entry_name: The Dataplex-linked resource name. We first look up the corresponding
                           Data Catalog entry. Example:
                           "//bigquery.googleapis.com/projects/PROJECT/datasets/DATASET/tables/TABLE"
        :param tag_template_name: The resource name of the tag template. Example:
                                  "projects/PROJECT_ID/locations/LOCATION_ID/tagTemplates/TEMPLATE_ID"
        :param fields: A dict of {field_id: value}, where field_id matches a field in the tag template,
                       and value is the data to set. The method inspects the field type and sets
                       the correct property (bool_value, enum_value, etc.).
        :return: The created Tag object.
        """
        # Prepare a new Tag object referencing the specified template
        tag = datacatalog_v1.types.Tag()
        tag.template = tag_template_name

        # Retrieve the tag template so we know each field's type
        template = self.client.get_tag_template(name=tag_template_name)
        template_fields = template.fields
        print(f"Found tag template: {template.name}, fields: {list(template_fields.keys())}")

        # Lookup the actual Data Catalog entry from the Dataplex-linked resource
        table_entry = self.client.lookup_entry(request={"linked_resource": entry_name})
        print(f"Data Catalog entry found: {table_entry.name}")

        # For each field in the 'fields' dict, set the correct TagField property
        for field_id, value in fields.items():
            if field_id not in template_fields:
                print(f"Warning: Field '{field_id}' not found in template '{tag_template_name}'. Skipping.")
                continue

            # Initialize a TagField
            tag.fields[field_id] = datacatalog_v1.TagField()
            tag_template_field = template_fields[field_id]

            # The FieldType object indicates whether it's a primitive, enum, etc.
            field_type = tag_template_field.type

            # Print debug info
            print(f"Processing field '{field_id}' with type '{field_type}' and value '{value}'")

            # RICHTEXT is handled as a special primitive in some versions, but usually you'll see
            # field_type.primitive_type = RICHTEXT or something else. Adjust if needed:
            if field_type.primitive_type == datacatalog_v1.FieldType.PrimitiveType.RICHTEXT:
                # Convert to simple HTML
                tag.fields[field_id].richtext_value = f"<p>{value}</p>"

            elif field_type.primitive_type == datacatalog_v1.FieldType.PrimitiveType.TIMESTAMP:
                # Parse a string in MM/DD/YYYY format into a datetime, then to a protobuf Timestamp
                dt = datetime.strptime(value, "%m/%d/%Y")
                ts = timestamp_pb2.Timestamp()
                ts.FromDatetime(dt)
                tag.fields[field_id].timestamp_value = ts

            elif field_type.enum_type:
                # It's an enum field. Must match exactly one of the allowed enum display names.
                tag.fields[field_id].enum_value.display_name = value

            elif field_type.primitive_type == datacatalog_v1.FieldType.PrimitiveType.BOOL:
                # Convert np.bool_ to Python bool if needed
                tag.fields[field_id].bool_value = bool(value)

            elif field_type.primitive_type == datacatalog_v1.FieldType.PrimitiveType.DOUBLE:
                # Both float and double_value are set with double_value
                tag.fields[field_id].double_value = float(value)

            elif field_type.primitive_type == datacatalog_v1.FieldType.PrimitiveType.STRING:
                # Check if it's rich text or normal string
                # Or if you want to detect HTML with string_utils, do so here
                tag.fields[field_id].string_value = str(value)

            else:
                print(f"Unsupported field type for '{field_id}'. "
                      f"Type: {field_type.primitive_type}. Skipping or handle as needed.")

        # Finally, create the tag in Data Catalog
        created_tag = self.client.create_tag(parent=table_entry.name, tag=tag)
        print(f"Tag created with name: {created_tag.name}")
        return created_tag
