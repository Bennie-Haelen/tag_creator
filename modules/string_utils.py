
def strip_if_str(x):
  """Function to strip leading/trailing whitespace if x is a string, otherwise return x"""
  return x.strip() if isinstance(x, str) else x

def strip_string_values(d):
    return {k: v.strip() if isinstance(v, str) else v for k, v in d.items()}


def cleanup_spreadsheet_column_name_header(header):
  
  column_name = header.encode('ascii', 'ignore').decode('ascii')
  column_name = column_name.replace('\u200b', '')

  return column_name

# Convert columns with non-standard boolean values to True/False
def convert_columns_to_boolean(df, columns):
    
     # Create a mapping from non-standard boolean values to True/False      
    boolean_mapping = {
        'yes': True,
        'no': False,
        'true': True,
        'false': False,
        '1': True,
        '1.0': True,
        '0.0': False,
        '0': False,
        'y': True,
        'n': False,
        'nan': False
    }
    for col in columns:    
        # Replace the original column values with the mapped True/False values
        df[col] = df[col].str.lower().map(boolean_mapping)

    return df

def is_html_string(input_string):
  """
  Checks if a string contains HTML tags.

  :param input_string: The string to check.
  :return: True if the string contains HTML tags, False otherwise.
  """
  if (input_string is None) or (not isinstance(input_string, str)):
      return False

  # In this case, we are using a regular expression to check 
  # if the string contains any HTML tags
  if input_string.startswith("<p>") and input_string.endswith("</p>"):
      return True
  else:
      return False
  

def clean_string(str_val: str) -> str:
    """
    Cleans a string by removing zero-width spaces, stripping whitespace, and converting to lowercase.

    :param col: The string value to clean.
    :return: The cleaned string value.
    """
    return str_val.replace('\u200b', '').strip().lower()