# start server on the current fpdb-store node

# import
this_dir=$(dirname "$0")
start_node_func_path=$(dirname "$this_dir")"/start_node_func.sh"
source "$start_node_func_path"

# call start_node()
start_node false
