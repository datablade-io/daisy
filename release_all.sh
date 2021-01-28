# clone sources and run a build
# git clone --depth=1 --recursive -b v20.11.1.4939-testing https://github.com/ClickHouse/ClickHouse.git

#git clone \
#    -b v20.11.1.4939-testing \
#    --depth=1 --shallow-submodules --recurse-submodules \
#    https://github.com/ClickHouse/ClickHouse.git
set +e
# set -x

APT_REPO="http://tob45.bigdata.lycc.qihoo.net:8080/public"
BUILD_PATH="$(pwd)"
PKG_PATH="$BUILD_PATH/output/"
GIT_REPO="ssh://git@tob45.bigdata.lycc.qihoo.net:2022/dae/daisy.git"

function log() {
  local format='+%Y/%m/%d %H:%M:%S%s'
  echo "[$(date "$format")]:: ${1}"
}

function get_source {
  # clone sources and run a build
  BRANCH=$1
  git clone -b "$BRANCH" "$GIT_REPO" "ClickHouse4" --depth 1
}

function upload_n_check {
  local file=$1
  local url=$2
  log "[INFO] Upload $file to $url"
  local status=$(curl --write-out %{http_code} --max-time 300 --retry 3 --retry-delay 0 --retry-max-time 300 --silent \
     -u "data-platform:bdg2020" -v \
    --upload-file "$file" "$url")
  if [[ "$status" -ne 200 ]]; then
    log "[ERROR] Upload $file fail"
  else
    log "[INFO] Upload $file succeed"
  fi
}

function post_n_check {
  local file=$1
  local url=$2
  log "[INFO] Upload $file to $url"
  local opt="--write-out %{http_code} --max-time 300 --retry 3 --retry-delay 0 --retry-max-time 300 --silent"
  local status=$(curl "$opt" \
     -u "data-platform:bdg2020" -v \
    -X POST "$url" -H "" --data-binary "@$file" )
  if [[ "$status" -ne 200 ]]; then
    log "[ERROR] Upload $file fail"
  else
    log "[INFO] Upload $file succeed"
  fi
}

function _upload_dir {
  local path=$1
  local pat=$2
  local baseUrl=$3

  pushd "$path" || return
  # check the version of *.deb
  for file in $(find "$path" -name "$pat")
  do
      log "[INFO] Upload $file"
      local url="$baseUrl/$(basename "$file")"
      if [[ "$pat" =~ "rpm" ]]; then
        upload_n_check "$file" "$url"
      else
        post_n_check "$file" "$baseUrl"
      fi
  done
  popd || return
}

function publish_deb {
  local deb_path=$1
  DEB_URL="http://registry.foundary.zone/repository/data-platform/"

  log "[INFO] Upload $deb_path/*.deb to $DEB_URL"
  _upload_dir "$deb_path" "*.deb" $DEB_URL
}

function publish_rpm {
  local path=$1
  RPM_URL="http://registry.foundary.zone/repository/data-platform-yum/main"

  log "[INFO] Upload $path/*.rpm to $RPM_URL"
  _upload_dir "$path" "*.rpm" $RPM_URL
}

function build_images {
 local DEB_VER=$1
  DOCKER_PATH="$BUILD_PATH/docker"
  DOCKER_REGISTRY_URL="local"
  DEB_REPO="deb http://registry.foundary.zone/repository/data-platform/ bionic main"
  DOCKER_REGISTRY_URL="http://registry.foundary.zone:8360"
  DOCKER_REGISTRY_HOST='registry.foundary.zone:8360'
  DOCKER_USER="data-platform"
  DOCKER_USER_PASSWORD="bdg2020"
  local TAG="$DOCKER_REGISTRY_HOST/daisy-server:$DEB_VER"

  docker build --network=host \
    --build-arg repository="$DEB_REPO" \
    --build-arg version="$DEB_VER" \
    -f "$DOCKER_PATH/server/Dockerfile" \
    -t "$TAG" \
    "$DOCKER_PATH/server"

  # publish image
  docker login -u "$DOCKER_USER" -p "$DOCKER_USER_PASSWORD" "$DOCKER_REGISTRY_URL"
  docker push "$TAG"
}

function build_binary {
  CCACHE_DIR="$(realpath ~/.ccache)"
  DEB_CC=${DEB_CC:=`which gcc-9 gcc | head -n1`}
  DEB_CXX=${DEB_CXX:=`which g++-9 g++ | head -n1`}

  DOCKER_PATH="$BUILD_PATH/docker"

  docker pull yandex/clickhouse-deb-builder:latest

  docker run --network=host --rm \
          --volume="$PKG_PATH":/output \
          --volume="$BUILD_PATH":/build \
          --volume="$CCACHE_DIR":/ccache \
          -e DEB_CC="$DEB_CC" \
          -e DEB_CXX="$DEB_CXX" \
          -e CCACHE_DIR=/ccache \
          -e CCACHE_BASEDIR=/build \
          -e CCACHE_NOHASHDIR=true \
          -e CCACHE_COMPILERCHECK=content \
          -e CCACHE_MAXSIZE=15G \
          -e ALIEN_PKGS='--rpm --tgz' \
          -e VERSION_STRING="$VERSION" \
          -e AUTHOR="$(whoami)" \
          -e CMAKE_FLAGS="$CMAKE_FLAGS -DADD_GDB_INDEX_FOR_GOLD=1 -DLINKER_NAME=lld" \
          yandex/clickhouse-deb-builder:latest

  # you can add into CMAKE_FLAGS some flags to exclude some libraries
  # -DENABLE_LIBRARIES=0 -DUSE_UNWIND=1 -DENABLE_JEMALLOC=1 -DENABLE_REPLXX=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_THINLTO=0 -DCMAKE_BUILD_TYPE=Release

  # clickhouse-deb-builder will write some files as root
  sudo chown -R $(id -u):$(id -g) "$BUILD_PATH"
}

function main {
  #get_source "DPT-348"
  build_binary
  VERSION=20.11.1.4939
  PKG_PATH="$BUILD_PATH/output/"
  publish_rpm "$PKG_PATH"
  publish_deb "$PKG_PATH"
  build_images "$VERSION"
}

main