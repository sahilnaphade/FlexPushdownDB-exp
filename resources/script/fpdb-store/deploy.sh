# Script to spread built package to all fpdb-store cluster nodes

# import
this_dir=$(dirname "$0")
deploy_func_path=$(dirname "$this_dir")"/deploy_func.sh"
source "$deploy_func_path"

# call deploy()
deploy false
