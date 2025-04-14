import xml.etree.ElementTree as ET
import json
import re

def transform_xml_to_array_of_JSON(xml_file):
    # Parse from file-like object
    tree = ET.parse(xml_file)
    root = tree.getroot()

    input_strings = []

    for elem in root.iter():
        full_tag = elem.tag
        attributes = elem.attrib
        text = elem.text.strip() if elem.text else "None"

        formatted_string = f"Tag: {full_tag}, Attributes: {attributes}, Text: {text}"
        input_strings.append(formatted_string)

    formatted_output = json.dumps(input_strings, indent=4)
    input_strings = json.loads(formatted_output)

    pattern = r"Tag: \{[^}]*\}(.*?)\s*,\s*Attributes: \{'contextRef': '([^']+)'(?:,\s*'decimals': '[^']+')?(?:,\s*'unitRef': '([^']+)')?[^}]*\},\s*Text:\s*(.+)"

    output = []

    for input_string in input_strings:
        preprocessed_text = input_string.replace("\n", " ")
        match = re.search(pattern, preprocessed_text)
        if match:
            tag_name = match.group(1)
            context_ref = match.group(2)
            unit_ref = match.group(3)
            text_value = match.group(4)
            output.append({
                "tag_name": tag_name,
                "context_ref": context_ref,
                "unit_ref": unit_ref,
                "text_value": text_value
            })

    return output
