from filer_names.dotcom_bubble import generate_dotcom_bubble_indicators
from filer_names.name_changes import generate_name_changes_indicators

if __name__ == "__main__":
    generate_dotcom_bubble_indicators(output_dir="data/filer_names")
    generate_name_changes_indicators(output_dir="data/filer_names")