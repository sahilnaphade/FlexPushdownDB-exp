# stop server on the current node

# import util
util_path=$(dirname "$0")"/util.sh"
source "$util_path"

# start server
server_pid_path="$deploy_dir"/"$exe_dir_name"/"$server_pid_name"
if [ -e "$server_pid_path" ]; then
	kill "$(cat "$server_pid_path")"
	rm "$server_pid_path"
fi
