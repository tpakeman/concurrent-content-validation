import os
import uuid
import json
import looker_sdk
from looker_sdk.sdk.api40 import models
from looker_sdk.rtl.transport import TransportOptions
from looker_sdk.error import SDKError
from datetime import datetime as dt

INI_FILE = os.path.join(os.getcwd(), 'looker.ini')

def print_time_est(t, max_s=10, precision=0):
    """Utility function to format a number of seconds in a nice way"""
    if t is None:
        return f""
    out_s = 'secs 😊'
    for d, s in [(60, 'mins 🥲'), (60, 'hrs 😬'), (24, 'days 🤯'), (7, 'weeks 💀')]:
        if t < (d * max_s):
            break
        else:
            out_s = s
            t /= d
    return f"{t:,.{precision}f} {out_s}"

class FolderTree(object):
    """
Class which takes a looker.ini section string and some optional kwargs
Produces a 'tree' of the Folders and their children
For each folder it calculates the estimated number of queries contained within
Args
--------------------
section:        str                 The ID of a string in a looker.ini file
print_progress:   bool (default True) Should scan progress be printed to the console 
est_interval:   int (default 10)    No. of folders after which to recalculate time remaining

Helpful Methods
--------------------
slice(n)        Return an array of length ~= total queries / n (i.e. 1/nth of queries on the instance)
    Each array is a dict in format {'content': []content_metadata_id, 'dashboards': []dashboard_id, 'looks': []look_id}
    This is designed to be passed in to a validator run filtered to a subset of content_metadata_ids
"""
    def __init__(self, section, print_progress=True):
        self.sdk = looker_sdk.init40(config_file=INI_FILE, section=section)
        s = dt.now()
        self.id = uuid.uuid4()
        self.tree = {}
        self.total_queries = 0
        self.total_folders = 0
        self.total_dashboards = 0
        self.total_looks = 0
        self._populate(print_progress)
        self._fetch_total_queries()
        e = dt.now()
        self.build_time = (e - s).total_seconds()
        if print_progress:
            print(self)

    def __str__(self):
        return f"""
Tree ID {self.id}
Built in {print_time_est(self.build_time)}
Consists of {self.total_folders:,} folders, containing:
    {self.total_dashboards:,} dashboards
    {self.total_looks:,} looks
    {self.total_queries:,} queries total
"""

    def _populate(self, print_progress):
        """Generate the tree of Looker folders"""
        s = dt.now()
        est = None
        res = self.sdk.all_folders(fields='id, parent_id, name, content_metadata_id')
        self.total_folders = len(res)
        for idx, folder in enumerate(res):
            if print_progress:
                prog = idx / self.total_folders
                if idx > 0:
                    t = (dt.now() - s).total_seconds()
                    est = (t / idx) * (self.total_folders - idx)
                outstr = f"Scanning folder {folder.id:<6} ({idx:^4,d}/{self.total_folders:^4,d}) ~{prog:<5.2%}"
                if est:
                    outstr += f" - {print_time_est(est)} remaining"
                print(outstr)
            cur = LookerFolder(folder, self.sdk, print_progress)
            self.tree[cur.id] = cur
            self.total_looks += len(cur.looks)
            self.total_dashboards += len(cur.dashboards)
        for folder in self.tree.values():
            if folder.parent_id is not None: 
                self.tree[folder.parent_id].add_child(folder)
    
    def _fetch_total_queries(self):
        """Calculate the total number of queries in the instance
        Must be run after the tree is generated"""
        for d in self.tree.values():
            self.total_queries += d.queries

    def _parse_tree(self, folder, threshold, query_ct=0, buffer={'content': [], 'dashboards': [], 'looks': []}, accumulator=[]):
        """Recursive function will traverses the tree from parent to children, maintaining a 
        running count of queries, looks and dashboards"""
        query_ct += folder.queries
        if folder.content_metadata_id:
            buffer['content'].extend(folder.fetch_parent_chain('content_metadata_id'))
            buffer['dashboards'].extend([d.id for d in folder.dashboards])
            buffer['looks'].extend(folder.looks)
        if query_ct >= threshold:
            data = {"queries": query_ct, "content_metadata": sorted(list(set(buffer['content'])), key=lambda x: int(x)), 'dashboards': buffer['dashboards'], 'looks': buffer['looks']}
            accumulator.append(data)
            buffer = {'content': [], 'dashboards': [], 'looks': []}
            query_ct = 0
        #TODO: only include in the path if has queries or children (and children have queries)
        #TODO: this will only work if content metadata is continually appended. If exact matching is used we will need
        # to reparse the tree to ensure no orphan content in a slice output
        for child in folder.children:
            query_ct, buffer, accumulator = self._parse_tree(child, threshold, query_ct, buffer, accumulator)
        return query_ct, buffer, accumulator

    def slice(self, n):
        """Return an array of dicts each containing ~1/nth of the instance queries"""
        threshold = (self.total_queries // n) + 1 if n > 1 else self.total_queries
        query_ct = 0
        buffer = {'content': [], 'dashboards': [], 'looks': []}
        accumulator = []
        for f in self.tree.values():
            if f.parent is None:
                query_ct, buffer, accumulator = self._parse_tree(f, threshold, query_ct, buffer, accumulator) 
        return accumulator


class LookerDashboard(object):
    """
Convenience class to wrap a LookerDashboard and calculate the number of 
queries it contains (dashboard elements with a `query` attribute)    
NOTE: LookML dashboards will raise a ValueError
"""
    def __init__(self, sdk_response):
        self.id = str(int(sdk_response.id)) # Raise a ValueError for IDs that cannot be coerced to ints - skipping LookML dashboards
        self.id = sdk_response.id
        self.queries = 0
        self.dashboard_elements = []
        self._calculate_queries(sdk_response)
    
    def _calculate_queries(self, sdk_response):
        if sdk_response.dashboard_elements:
            for el in sdk_response.dashboard_elements:
                self.dashboard_elements.append(el.id)
                if el.query:
                    self.queries += 1


class LookerFolder(object):
    """
Class to wrap a Looker Folder. Uses the SDK to fetch the enclosed dashboards and looks
Can be associated with other Looker Folders as children or parents
"""
    def __init__(self, sdk_response, sdk, print_progress=False):
        self.sdk = sdk
        self.print_progress = print_progress
        self.id = sdk_response.id
        self.name = sdk_response.name
        self.content_metadata_id = sdk_response.content_metadata_id
        self.parent = None
        self.parent_id = sdk_response.parent_id
        self.children = []
        self.looks = []
        self.dashboards = []
        self.queries = 0
        self.child_queries = 0
        self.fetch_content()
        self.calculate_child_queries()
    
    def __str__(self):
        out_s = f"Folder: {self.name} ({self.id}) - # children: {len(self.children)}"
        if self.parent_id:
            out_s += f' - parent ID: {self.parent_id}'
        if self.queries > 0:
            out_s += f' - total_queries: {self.queries}'
        return out_s

    def fetch_parent_chain(self, target='content_metadata_id'):
        """Fetch the chain of properties for the parents of this folder"""
        ALLOWED = self.__dict__.keys()
        if target not in ALLOWED:
            raise ValueError(f"Target must be one of {ALLOWED.join(',')}")
        cur = self
        buffer = []
        while True:
            buffer.append(cur.__dict__[target])
            if cur.parent is not None:
                cur = cur.parent
            else:
                break
        return list(set(buffer))

    def fetch_content(self):
        dr = self.sdk.folder_dashboards(self.id, fields='id, dashboard_elements')
        lr = self.sdk.folder_looks(self.id, fields='id')
        for d in dr:
            if self.print_progress:
                print(f"\tdashboard {d.id} found")
            self._add_dashboard(d)
        for l in lr:
            if self.print_progress:
                print(f"\tlook {l.id} found")
            self._add_look(l.id)

    def _add_dashboard(self, sdk_response):
        try:
            d = LookerDashboard(sdk_response)
            self.dashboards.append(d)
            self.queries += d.queries
        except ValueError: # skip LookML dashboards
            if self.print_progress:
                print(f"Skipped LookML dashboard {sdk_response.id}")
    
    def _add_look(self, id):
        self.looks.append(id)
        self.queries += 1

    def _add_parent(self, parent):
        """Add a link to another LookerFolder"""
        self.parent = parent
    
    def add_child(self, child):
        """Add a child object to the self.children array"""
        self.children.append(child)
        child._add_parent(self)
    
    def calculate_child_queries(self):
        """Iterate through all children and return the total number of queries
        represented by the current folder and all descendents."""
        total = self.queries
        for c in self.children:
            total += c.calculate_child_queries()
        self.child_queries = total
        return total


class ValidatorRunner(object):
    """Pass in create_users or target_user"""
    def __init__(self, max_timeout=600, create_users=False, target_user=None, sdk=None, section=None, print_progress=True):
        self.create_users = create_users
        self.target_user = str(target_user)
        self.max_timeout = max_timeout
        self.authed_users = []
        self.print_progress = print_progress
        self.results = {}
        if (not sdk) and section:
            self.sdk = looker_sdk.init40(config_file=INI_FILE, section=section)
        else:
            self.sdk = sdk
        self._fetch_develop_groups_roles()
        self.long_timeout = TransportOptions(timeout=self.max_timeout)
        self.metadata_added = {}
        
    def _fetch_develop_groups_roles(self):
        self.sdk.auth.logout()
        groups = self.sdk.search_groups_with_roles(fields='id,roles(id,permission_set(permissions))')
        roles = self.sdk.all_roles(fields='id,permission_set(permissions)')
        self.target_groups = [g.id for g in groups for r in g.roles if 'develop' in r.permission_set.permissions]
        self.target_roles = [r.id for r in roles if 'develop' in r.permission_set.permissions]

    def _check_fix_access(self, target_user):
        if target_user in self.authed_users:
            return
        else:
            user_info =self.sdk.user(target_user, fields='group_ids,role_ids')
            for g in user_info.group_ids:
                if g in self.target_groups:
                    break
            for r in user_info.role_ids:
                if r in self.target_roles:
                    break
            self.authed_users.append(target_user)

    def _run_validation(self, target_user):
        self._check_fix_access(target_user)
        self.sdk.auth.login_user(target_user)
        s = dt.now()
        self.sdk.content_validation(transport_options=self.long_timeout)
        t = (dt.now() - s).total_seconds()
        self.sdk.auth.logout()
        return t

    def run_validation_from_slices(self, slices, iterations=1):
        total_scanned = 0
        for slice in slices:
            total_scanned += slice['queries']
            for idx in range(iterations):
                if self.target_user:
                    self._amend_content_metadata(self.target_user, slice['content_metadata'])
                    result = self._run_validation(self.target_user)
                    if self.print_progress:
                        print(f"Run ({idx + 1}/{iterations}) for {total_scanned} queries: completed in {print_time_est(result)}")
                    if total_scanned in self.results:
                        self.results[total_scanned].append(result)
                    else: 
                        self.results[total_scanned] = [result]
                elif self.create_users:
                    #TODO make it possible to create new users and clean up afterwards(?)
                    ...
                else:
                    raise ValueError("Must either supply a target user or create_users=True")
    
    def print_results(self, total):
        for queries, results in self.results.items():
            frac = queries / total
            avg = sum(results) / len(results)
            print(f"{frac:.2%} of queries scanned in avg. {print_time_est(avg, precision=2)} ({len(results)} iterations)")

    def _amend_content_metadata(self, target_user, metadata_ids):#, exact=False): # can't see an efficient way to implement this
        """Assign the content metadata accesses to the target users.
        passing exact=True will ensure the user ONLY has the exact IDs 
        passed in"""
        #TODO! pseudocode as the first SDK method doesn't exist
        # existing = get_current_metadatas(target_user)
        # if exact:
        #     for metadata in existing:
        #         if metadata not in metadata_ids:
        #             # sdk.delete_content_metadata_access()
        #             ...
        if target_user not in self.metadata_added:
            self.metadata_added[target_user] = []
        for metadata in metadata_ids:
            # if metadata not in existing:
            if metadata not in self.metadata_added[target_user]:
                body = models.ContentMetaGroupUser(
                    user_id=target_user,
                    content_metadata_id=metadata,
                    permission_type='view'
                )
                try:
                    self.sdk.create_content_metadata_access(body)
                    self.metadata_added[target_user].append(metadata)
                except SDKError as e:
                    message = json.loads(e.args[0])['message']
                    if 'already has access' in message:
                        continue
                    else:
                        print(f"Content Metadata ID {metadata} failed with message '{message}'")
                        # raise e # TODO: distinguish valid and invalid Exceptions
                        continue
