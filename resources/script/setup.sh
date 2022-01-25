# main entry to setup the system on a cluster

# parameters
install_dependency=false

# get script path
pushd "$(dirname "$0")" > /dev/null
script_dir=$(pwd)
popd > /dev/null

# directories
resource_dir="$(dirname "${script_dir}")"
root_dir="$(dirname "${resource_dir}")"

# install dependencies
if [ "$install_dependency" = true ]; then
  if [ "$(uname)" = "Darwin" ]; then
    # mac OS
    echo "Warning: automatically installing dependencies on Mac OS is unsupported."
  elif [ "$(expr substr "$(uname -s)" 1 5)" == "Linux" ]; then
    # linux
    if [  -n "$(uname -a | grep Ubuntu)" ]; then
      # ubuntu
      source "$root_dir""/tools/project/bin/ubuntu-prerequisites.sh"
    else
      echo "Warning: automatically installing dependencies on Linux which is not Ubuntu is unsupported."
    fi
  else
    echo "*** Error: supported OS are Mac OS or Linux."
    exit 0
  fi
fi

# build
source "$script_dir""/build.sh"

# deploy
source "$script_dir""/deploy.sh"
