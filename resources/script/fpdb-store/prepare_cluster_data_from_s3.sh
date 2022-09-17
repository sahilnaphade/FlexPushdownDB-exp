# download data from s3 on all storage nodes

# import util
util_param_path=$(dirname "$0")"/util_param.sh"
source "$util_param_path"
util_func_path="$script_dir""/util_func.sh"
source "$util_func_path"

# configurable parameters
data_relative_dirs=("tpch-sf0.01")

# download data on each storage node, for downloading each dir, nodes can do in parallel
for data_relative_dir in "${data_relative_dirs[@]}"
do
  echo -n "  Downloading data for ""$node_ip""... "
  check_or_add_to_known_hosts "$node_ip"

  # download in parallel
  pids=()
  for node_ip in "${node_ips[@]}"
  do
    run_command "$pem_path" "$node_ip" "$this_file_dir""/prepare_data_from_s3.sh" "$data_relative_dir" "&"
    pids[${#pids[@]}]=$!
  done

  # wait
  for pid in "${pids[@]}"
  do
      wait $pid
  done
done
