# stop server on the current fpdb-store node

# import
this_dir=$(dirname "$0")
stop_node_func_path=$(dirname "$this_dir")"/stop_node_func.sh"
source "$stop_node_func_path"

# call stop_node()
stop_node false
