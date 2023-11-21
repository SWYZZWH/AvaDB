def convert_file_to_utf8(original_file, new_file=None):
    if new_file is None:
        new_file = original_file

    # Read the original file with ISO-8859-1 encoding
    with open(original_file, 'r', encoding='ISO-8859-1') as file:
        contents = file.read()

    # Write the contents to the new file with UTF-8 encoding
    with open(new_file, 'w', encoding='utf-8') as file:
        file.write(contents)


# Usage
original_file = 'XboxOne_GameSales.csv'
new_file = 'xboxSales.csv'  # Optionally, specify a new file name
convert_file_to_utf8(original_file, new_file)
