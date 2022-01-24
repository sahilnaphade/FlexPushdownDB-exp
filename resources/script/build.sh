# script to build the system locally

trap ctrl_c INT
function ctrl_c() {
  echo "*** Trapped CTRL-C, exit"
  popd > /dev/null
  exit 0
}

# parameters
build_dir_name="build"
clean=true
build_parallel=8
targets=("normal-frontend-server" "normal-frontend-test")

# get script path
pushd "$(dirname "$0")" > /dev/null
script_dir=$(pwd)

# make build directory
resource_dir="$(dirname "${script_dir}")"
root_dir="$(dirname "${resource_dir}")"
build_dir="${root_dir}"/"${build_dir_name}"

if [ "${clean}" = true ]; then
  rm -rf "${build_dir}"
fi
mkdir -p "${build_dir}"

# load cmake
if [ "$(uname)" = "Darwin" ]; then
  # mac OS
  C_compiler="/usr/local/opt/llvm@13/bin/clang-13"
  CXX_compiler="/usr/local/opt/llvm@13/bin/clang++"
elif [ "$(expr substr "$(uname -s)" 1 5)" == "Linux" ]; then
  # linux
  C_compiler="/usr/bin/clang-12"
  CXX_compiler="/usr/bin/clang++-12"
else
  echo "*** Error: supported OS are Mac OS or Linux."
  exit 0
fi

cd "${build_dir}"
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER="${C_compiler}" -DCMAKE_CXX_COMPILER="${CXX_compiler}" \
-G "CodeBlocks - Unix Makefiles" "${root_dir}"

# build targets
for target in "${targets[@]}"
do
  cmake --build . --target "${target}" -- -j "${build_parallel}"
done

# back to original directory
popd > /dev/null
