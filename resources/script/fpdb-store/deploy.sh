# Script to spread built package to all fpdb-store cluster nodes

# get script path
pushd "$(dirname "$0")" > /dev/null
this_dir=$(pwd)
script_dir=$(dirname "$this_dir")
popd > /dev/null

# import
util_param_path="$script_dir""/util_param.sh"
source "$util_param_path"
util_func_path="$script_dir""/util_func.sh"
source "$util_func_path"
deploy_func_path="$script_dir""/deploy_func.sh"
source "$deploy_func_path"

# call deploy()
deploy false
