import json

def read_snapshot_groups(file_path):
    with open(file_path, 'r') as file:
        contents = file.read()
        data = json.loads(contents)
        filtered_parent_groups = [group for group in data['parentGroups'] if group.startswith('datasnapshot') and group.endswith('steward')]
        modified_groups = [group[len('datasnapshot_'):-len('_steward')] for group in filtered_parent_groups]
        with open(output_file_path, 'w') as output_file:
            json.dump(modified_groups, output_file, indent=4)

if __name__ == "__main__":
    file_path = '../setupResourceScripts/snapshot_groups'
    output_file_path = '../setupResourceScripts/snapshot_ids.json'
    read_snapshot_groups(file_path)
