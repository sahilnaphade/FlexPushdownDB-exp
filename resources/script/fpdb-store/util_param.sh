
# get script path
pushd "$(dirname "$0")" > /dev/null
this_file_dir=$(pwd)
script_dir="$(dirname "${this_file_dir}")"
popd > /dev/null

# configurable parameters
export pem_path="$HOME""/.aws/yifei-aws-wisc.pem"

# directories
resource_dir="$(dirname "${script_dir}")"
root_dir="$(dirname "${resource_dir}")"

# node ips
node_ips_path="${resource_dir}""/config/fpdb-store_ips"
while IFS= read -r line || [[ -n "$line" ]];
do
  node_ips+=("$line")
done < "$node_ips_path"
export node_ips

echo $script_dir