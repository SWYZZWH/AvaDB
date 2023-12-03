import os
import json
import argparse


def read_and_escape_file_contents(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Escape backslashes and double quotes for JSON compatibility
            return file.read().replace('\\', '\\\\').replace('"', '\\"')
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None


def combine_files(root_dir, script_name, skip_dirs):
    combined_files = []

    for root, dirs, files in os.walk(root_dir):
        # Skip specified directories
        dirs[:] = [d for d in dirs if d not in skip_dirs]

        for file in files:
            if file.endswith('.py') or file.endswith('.md'):
                # Skip the script itself
                if file == script_name:
                    continue

                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, root_dir)
                content = read_and_escape_file_contents(full_path)

                if content is not None:
                    combined_files.append({"location": relative_path, "content": content})

    return combined_files


def main():
    with open("gpt_source.json", "w") as f:
        f.write("")

    parser = argparse.ArgumentParser(description='Combine Python and Markdown files into a JSON structure.')
    parser.add_argument('root_dir', type=str, help='Root directory of the project')
    parser.add_argument('-s', '--skip', nargs='*', help='Directories to skip', default=[])
    args = parser.parse_args()

    script_name = os.path.basename(__file__)
    combined_data = combine_files(args.root_dir, script_name, set(args.skip))
    json_data = json.dumps({"files": combined_data}, indent=4)

    with open("gpt_source.json", "w") as f:
        f.write(json_data)


if __name__ == "__main__":
    main()
