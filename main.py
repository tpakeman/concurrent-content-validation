from argparse import ArgumentParser
from validator.models import FolderTree, ValidatorRunner

def cli():
    """Parse input arguments"""
    parser = ArgumentParser()
    parser.add_argument('section', nargs='+', help="Name a section of the looker.ini file to auth into")
    parser.add_argument('--silent', '-s', action='store_true', help="Supress progress as the folder tree is scanned")
    parser.add_argument('--user', '-u', type=int, help="The ID of the user to impersonate for validator runs")
    parser.add_argument('--create_users', '-c', action='store_true', help="Flag to create users rather than use a named user")
    parser.add_argument('--timeout', '-t', type=int, default=600, help="Set a max timeout for content validator runs")
    parser.add_argument('--fractions', '-f', type=int, default=10, help="How many equal sized fractions of the content should be validated in each run")
    parser.add_argument('--iterations', '-i', type=int, default=1, help="How many times should each validator run execute")
    args = parser.parse_args()
    print(args)
    if not (args.create_users or args.user):
        raise ValueError("Must either name a user or choose to create users.\nNOTE - creating users is not yet implemented")
    return args

def main():
    """Parse the folder tree, divide the content into slices, validate"""
    args = cli()
    section_with_spaces = ' '.join(args.section)
    tree = FolderTree(section_with_spaces, print_progress=(not args.silent))
    validator = ValidatorRunner(target_user=str(args.user), create_users=args.create_users, sdk=tree.sdk, max_timeout=args.timeout) 
    validator.run_validation_from_slices(tree.slice(args.fractions), iterations=args.iterations)


if __name__ == '__main__':
    main()