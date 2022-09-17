# download data from s3 on all storage nodes

# import util
util_param_path=$(dirname "$0")"/util_param.sh"
source "$util_param_path"
util_func_path="$script_dir""/util_func.sh"
source "$util_func_path"

# configurable parameters
data_relative_dirs=("tpch-sf0.01")

# download data on each storage node
for node_ip in "${node_ips[@]}"
do
  echo -n "  Downloading data for ""$node_ip""... "
  check_or_add_to_known_hosts "$node_ip"
  run_command "$pem_path" "$node_ip" "$this_file_dir""/prepare_data_from_s3.sh"
done
