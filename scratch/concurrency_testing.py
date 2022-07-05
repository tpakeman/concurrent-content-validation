"""Runs Content Validator."""
# sort out dev branch behaviour
# fix weird profservices hardcode behaviour
# make faster

import looker_sdk
from looker_sdk import models
import hashlib
import argparse
import csv
from datetime import datetime as dt
from concurrent.futures import Future, ThreadPoolExecutor

def timer(fn):
    """Time a function in seconds"""
    def wrapper(*args, **kwargs):
        s = dt.now()
        r = fn(*args, **kwargs)
        e = dt.now()
        t = (e - s).total_seconds()
        print(f"Function {fn.__name__} ran in {t:>5.2f}s")
        return r

    return wrapper


@timer
def main(section=None, num_threads=1, print_progress=False):
    """Compare the output of content validator runs
    in production and development mode. Additional
    broken content in development mode will be
    outputted to a csv file.
    Use this script to test whether LookML changes
    will result in new broken content."""

    parser = argparse.ArgumentParser(description='Run content validator')
    parser.add_argument('project', type=str,
                        help='name of project to validate. This arg is required.')
    parser.add_argument('--branch', '-b', type=str,
                        help='Name of branch you want to validate. If ommited this will use prod.')
    args = parser.parse_args()
    sdk = looker_sdk.init40(section=section)
    sdk2 = looker_sdk.init40(section=section)
    sdk2 = checkout_dev_branch(sdk2, args.branch, args.project)
    base_url = sdk._path('me').split('/api/')[0]
    folder_data = get_folder_data(sdk)

    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        if print_progress:
            print("Checking for broken content in production and on dev branch.")
        broken_content_prod = pool.submit(parse_broken_content, base_url, get_broken_content(sdk), folder_data)
        broken_content_dev = pool.submit(parse_broken_content, base_url, get_broken_content(sdk2), folder_data)
    while True:
        if all([f.done() for f in [broken_content_prod, broken_content_dev]]):
            new_broken_content = compare_broken_content(broken_content_prod.result(), broken_content_dev.result())
            break
    if new_broken_content:
        if print_progress:
            print(new_broken_content)
        write_broken_content_to_file(new_broken_content, "new_broken_content.csv")
    else:
        if print_progress:
            print("No new broken content in development branch.")
    broken = len(new_broken_content)
    assert broken == 0


def get_folder_data(sdk):
    """Collect all folder information"""
    folder_data = sdk.all_folders(fields="id, parent_id, name")
    return folder_data


def get_broken_content(sdk):
    """Collect broken content"""
    broken_content = sdk.content_validation(
        transport_options={"timeout": 6000}
    ).content_with_errors
    return broken_content


def parse_broken_content(base_url, broken_content, folder_data):
    """Parse and return relevant data from content validator"""
    output = []
    for item in broken_content:
        content_type = "dashboard" if item.dashboard else "look"
        item_content_type = getattr(item, content_type)
        try:
            id = item_content_type.id
            name = item_content_type.title
            folder_id = item_content_type.folder.id
            folder_name = item_content_type.folder.name
            errors = item.errors
            url = f"{base_url}/{content_type}s/{id}"
            folder_url = f"{base_url}/folders/{folder_id}"
        except AttributeError:
            print(f"{item} has no id...")
            pass
        if content_type == "look":
            element = None
        else:
            dashboard_element = item.dashboard_element
            element = dashboard_element.title if dashboard_element else None
        # Lookup additional folder information
        folder = next(i for i in folder_data if str(i.id) == str(folder_id))
        parent_folder_id = folder.parent_id
        # Old version of API  has issue with None type for all_folder() call
        if parent_folder_id is None or parent_folder_id == "None":
            parent_folder_url = None
            parent_folder_name = None
        else:
            parent_folder_url = f"{base_url}/folders/{parent_folder_id}"
            parent_folder = next(
                (i for i in folder_data if str(i.id) == str(parent_folder_id)), None
            )
            # Handling an edge case where folder has no name. This can happen
            # when users are improperly generated with the API
            try:
                parent_folder_name = parent_folder.name
            except AttributeError:
                parent_folder_name = None
        # Create a unique hash for each record. This is used to compare
        # results across content validator runs
        unique_id = hashlib.md5(
            "-".join(
                [str(id), str(element), str(name), str(errors), str(folder_id)]
            ).encode()
        ).hexdigest()
        data = {
            "unique_id": unique_id,
            "content_type": content_type,
            "name": name,
            "url": url,
            "dashboard_element": element,
            "folder_name": folder_name,
            "folder_url": folder_url,
            "parent_folder_name": parent_folder_name,
            "parent_folder_url": parent_folder_url,
            "errors": str(errors),
        }
        output.append(data)
    return output


def compare_broken_content(broken_content_prod, broken_content_dev):
    """Compare output between 2 content_validation runs"""
    unique_ids_prod = set([i["unique_id"] for i in broken_content_prod])
    unique_ids_dev = set([i["unique_id"] for i in broken_content_dev])
    new_broken_content_ids = unique_ids_dev.difference(unique_ids_prod)
    new_broken_content = []
    for item in broken_content_dev:
        if item["unique_id"] in new_broken_content_ids:
            new_broken_content.append(item)
    return new_broken_content


def checkout_dev_branch(sdk, branch, project):
    """Enter dev workspace"""
    sdk.update_session(models.WriteApiSession(workspace_id="dev"))
    sdk.update_git_branch(project_id=project,
                          body=models.WriteGitBranch(name=branch))
    return sdk


def write_broken_content_to_file(broken_content, output_csv_name):
    """Export new content errors in dev branch to csv file"""
    try:
        with open(output_csv_name, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(broken_content[0].keys()))
            writer.writeheader()
            for data in broken_content:
                writer.writerow(data)
        print("Broken content information outputed to {}".format(output_csv_name))
    except IOError:
        print("I/O error")

if __name__ == '__main__':
    for n in [8, 1]:
        print(f"Running with {n} thread(s)")
        main(section='Profservices', num_threads=n, print_progress=True)