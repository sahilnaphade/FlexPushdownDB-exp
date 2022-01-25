# main entry to setup the system on a cluster

# get script path
pushd "$(dirname "$0")" > /dev/null
script_dir=$(pwd)
popd > /dev/null

# build
source "$script_dir""/build.sh"

# deploy
source "$script_dir""/deploy.sh"