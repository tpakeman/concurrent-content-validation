from argparse import ArgumentParser
from validator.models import FolderTree, ValidatorRunner

def cli():
    """Parse input arguments"""
    parser = ArgumentParser()
    parser.add_argument('section', nargs='+', help="Name a section of the looker.ini file to auth into")
    parser.add_argument('--print', '-p', action='store_false', help="")
    parser.add_argument('--user', '-u', type=int, help="The ID of the user to impersonate for validator runs")
    parser.add_argument('--create_users', '-c', action='store_false', help="Flag to create users rather than use a named user")
    parser.add_argument('--timeout', '-t', type=int, default=600, help="Set a max timeout for content validator runs")
    parser.add_argument('--fraction', '-f', type=int, default=20, help="What fraction of the content should be validated in each run")
    parser.add_argument('--iterations', '-i', type=int, default=1, help="How many times should each validator run execute")
    return parser.parse_args()

def main():
    args = cli()
    tree = FolderTree(' '.join(args.section), print_progress=args.print)
    validator = ValidatorRunner(target_user=str(args.user), sdk=tree.sdk, max_timeout=args.timeout) 
    validator.run_validation_from_slices(tree.slice(args.fraction), iterations=args.iterations)


if __name__ == '__main__':
    main()