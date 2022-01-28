# stop the system on all cluster nodes

# import util
util_path=$(dirname "$0")"/util.sh"
source "$util_path"

# stop server on each slave node
echo "Stopping server on cluster nodes..."

for slave_ip in "${slave_ips[@]}"
do
  echo -n "  Stopping ""$slave_ip""... "
  check_or_add_to_known_hosts "$slave_ip"
  run_command "$pem_path" "$slave_ip" "$script_dir""/stop_node.sh"
  echo "  done"
done

echo "done"
