# ./scripts notes

This directory contains scripts that are meant to make developer's lives easier.

Each of the files in this directory follow a common pattern:

```shell
./scripts/cli [-h|--help]
```

where `./scripts/cli` represents the command being executed.

The rest of this document includes notes about script setup and common patterns.
It was decided to use `#!/usr/bin/env bash` as a standard scripting language to minimize
dependencies.

Please see
[The Broad's repo guidelines for automation](https://broadworkbench.atlassian.net/wiki/spaces/TLR/pages/2952298505/GitHub+Repo+Standards#Repository-automation-standards)
for more information on developer convenience scripts.

## Setting up `usage()`

At the top of each script there should exist a `usage() {...}` bash function.
This serves to self-document the script and ensure the help norm is being followed for all scripts.

## Handling commandline options

Copy-and-paste the code below for handling command-line options in bash
without requiring extra installs.

The quick summary for using this approach:

- `getopts` argument syntax is as follows
  - a list of short option characters to look for
  - if your option requires a value, it must be followed by a `:`
  - if you are supporting `--long-names`, the `-:` must be included
- `die` and `needs_arg` are convenience methods for killing your script and verifying that a
  long-name option was passed

**SUPPORTED** option syntaxes are:

- `cli -a`=> specified with `getopts a`
- `cli -a option-value`=> specified with `getopts a:`
- `cli -aoption-value` => specified with `getopts a:`
- `cli --long-name=option-value` => specified with `getopts -:`

**UNSUPPORTED** option syntax(es):

- `cli --long-name option-value` => will not capture the option-value
  - the caveat is that `getopts` assumes the `OPTARG` is `-long-name`,
    akin to the `-aoption-value` assignment above.

## `init.sh`

We use this entrypoint for all common `env`-ironment variables and `scripts/`-based set up.

### `utils.sh`

This script serves as a library of tools that can be used in shell scripts to help with logging.
See [./scripts/utils.sh](./utils.sh) for usage and more information.

These utilities are loaded via `init.sh`.

## Sample hello-world script

Please copy-and-paste the following into your script to get you started.

```shell
#!/usr/bin/env bash
# this script is here to help get you started
# ACTIONS:
# - update usage() below
# - update/remove script variable defaults,
# - update `getopts` argument
#   (currently `ab:ch-:`, you'll want to delete `ab:c` and leave the rest),
# - update/delete these comments, and
# - happy scripting!

usage() {
  echo "usage: $0 [-a|--alpha] [-b|--bravo] [-c|--charlie] [-h|--help] ARG1"
  echo "[-a|--alpha]     example of an option with no value"
  echo "[-b|--bravo]     example of an option taking a value"
  echo "[-c|--charlie]   example of an option and a default [default: $charlie_default]"
  echo "[-h|--help]      print this help text"
  echo "ARG1             just an argument to process"
}

# script variables and defaults
# replace this and include it with a call to `init.sh` (if one exists)
source $(dirname $0)/utils.sh

bravo="$HOME/Downloads"       # Overridden by the value set by -b or --bravo
charlie_default="brown"       # Only set given -c or --charlie without an arg
ARG1=""


# process command-line options (if any)
die() { log_error "$*" >&2; echo ""; usage; exit 2; }  # complain to STDERR and exit with error
needs_arg() { if [ -z "$OPTARG" ]; then die "No arg for --$OPT option"; fi; }

while getopts ab:ch-: OPT; do  # allow -a, -b with arg, -c, and -- "with arg"
  # support long options: https://stackoverflow.com/a/28466267/519360
  if [ "$OPT" = "-" ]; then   # long option: reformulate OPT and OPTARG
    OPT="${OPTARG%%=*}"       # extract long option name
    OPTARG="${OPTARG#$OPT}"   # extract long option argument (may be empty)
    OPTARG="${OPTARG#=}"      # if long option argument, remove assigning `=`
  fi
  case "$OPT" in
    a | alpha )    alpha=true ;;
    b | bravo )    needs_arg; bravo="$OPTARG" ;;
    c | charlie )  charlie="${OPTARG:-$charlie_default}" ;;  # optional argument
    h | help )     usage; exit 0 ;;
    \? )           usage; exit 2 ;;  # bad short option (error reported via getopts)
    * )            die "Illegal option --$OPT" ;;            # bad long option
  esac
done
shift $((OPTIND-1)) # remove parsed options and args from $@ list


# process positional arguments (if any)
ARG1=$1

log_error "Hello world!"
log_info "alpha:   ${alpha}"
log_info "bravo:   ${bravo}"
log_info "charlie: ${charlie}"
log_debug "ARG1:    ${ARG1}"
```
