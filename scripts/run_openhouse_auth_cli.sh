# Script to setup and execute Openhouse token CLI

# Declare env variables for the current shell
ARTIFACTORY_URL=https://artifactory.corp.linkedin.com:8083/artifactory/
GROUP_NAME=com.linkedin.li-openhouse
ARTIFACT=auth-token-cli
OPENHOUSE_CLI_HOME="${OPENHOUSE_CLI_HOME:-}"
OPENHOUSE_CLI_CONFIG_PATH=""

# Download and extract latest openhouse cli distribution
function download_and_extract() {
  # If OPENHOUSE_CLI_HOME is already set and valid, skip download
  if [ -n "${OPENHOUSE_CLI_HOME}" ] && [ -d "${OPENHOUSE_CLI_HOME}" ]; then
    echo_with_time "OPENHOUSE_CLI_HOME already set to: ${OPENHOUSE_CLI_HOME}"
    echo_with_time "Skipping download, using local installation"
    return 0
  fi

  local latest_version_url="${ARTIFACTORY_URL}api/search/latestVersion?g=${GROUP_NAME}&a=${ARTIFACT}"
  echo_with_time $latest_version_url
  local version=$(curl -XGET -s $latest_version_url)
  echo_with_time "Latest distribution version: ${version}"
  # Check if the openhouse cli directory with the latest version exists. If not download the latest distribution
  if [ ! -d "${ARTIFACT}-${version}" ]; then
    echo_with_time "Downloading the latest distribution version: ${version}"
    local tar_distribution="${ARTIFACT}-${version}.tar"
    local download_url="${ARTIFACTORY_URL}TOOLS/com/linkedin/li-openhouse/${ARTIFACT}/${version}/${tar_distribution}"
    echo_with_time $download_url
    wget -q --timeout=60 $download_url
    if [ $? -ne 0 ]; then
      echo_with_time "Download failed. Exiting."
      exit 1
    fi
    echo_with_time "Extracting the distribution: ${tar_distribution}"
    tar -xf ${tar_distribution}
    rm -rf ${tar_distribution}
  fi
  # Set openhouse token cli home dir
  OPENHOUSE_CLI_HOME="${HOME}/${ARTIFACT}-${version}"
}

# Function to print usage
function usage() {
  echo "$0 is for fetching openhouse auth token"
  echo "Usage:"
  echo "sh $0 [-e|--executingIdentity headless/ldap] [-r|--realIdentity ldap/servicePrincipal] [-f|--tokenFile tokenFile] [-k|--keystore keystore]"
  echo ""
  echo "    -e|--executingIdentity    The executing identity i.e. Headless account/ldap user."
  echo ""
  echo "    -r|--realIdentity         Real identity urn i.e. service certificate principal or user principal. For example, urn:li:userPrincipal:user. Default: ldap user."
  echo ""
  echo "    -f|--tokenFile            Writes Openhouse auth token to this location. If not provided writes to a default tmp location i.e. /tmp/token_[uuid]"
  echo ""
  echo "    -k|--keystore             Grestin certificate location provided to KSudo as a proof of identity. If not provided user is
                                      automatically prompted to provide LDAP user and password"
  echo "    -c|--cluster              The openhouse cluster for which auth token will be generated. Default cluster is ltx1-holdem.
                                      Specify lva1-war to generate token for War cluster and ltx1-faro for ei cluster"
  echo ""
  echo "    -stacktrace               Print stacktrace of errors."
  echo ""
  exit 1
}

# echo with current timestamp
echo_with_time() {
  timestamp=$(date +"%Y-%m-%d %T")
  echo "$timestamp" "$@"
}

# This script is intended to run on gateway machines as well as openhouse jobs scheduler pods.
# The java installation location is /export/apps/jdk/ for both and so setting this as java base path.

JAVA_BASE="/export/apps/jdk"   # change this  if needed

# Function to pick latest matching Java version from a directory
pick_java_version() {
  local version_pattern=$1
  ls -d $JAVA_BASE/*${version_pattern}* 2>/dev/null | sort -V | tail -1
}

# Function to check if JAVA_HOME points to Java 17 or 11
is_valid_java_home() {
  if [ -z "$JAVA_HOME" ]; then
    return 1
  fi

  if [ ! -x "$JAVA_HOME/bin/java" ]; then
    return 1
  fi

  local version_output
  version_output=$("$JAVA_HOME/bin/java" -version 2>&1)

  if echo "$version_output" | grep -q 'version "17'; then
    echo_with_time "Java 17 is already set to JAVA_HOME"
    return 0
  fi
  if echo "$version_output" | grep -q 'version "11'; then
    echo_with_time "Java 11 is already set to JAVA_HOME"
    return 0
  fi

  return 1
}

# Function to setup java home
setup_java_home() {
  echo "Checking existing JAVA_HOME..."

  # If JAVA_HOME is already set AND points to Java 17 or 11, keep it
  if is_valid_java_home; then
    echo "Existing JAVA_HOME is valid: $JAVA_HOME"
    "$JAVA_HOME/bin/java" -version
    return 0
  else
    echo "JAVA_HOME is not set or not Java 17/11. Selecting version..."
  fi

  # Auto-select Java 17, then Java 11
  JAVA17_PATH=$(pick_java_version "17")

  if [ -n "$JAVA17_PATH" ]; then
    JAVA_HOME="$JAVA17_PATH"
    echo "Java 17 selected: $JAVA_HOME"
  else
    JAVA11_PATH=$(pick_java_version "11")
    if [ -n "$JAVA11_PATH" ]; then
      JAVA_HOME="$JAVA11_PATH"
      echo "Java 11 selected: $JAVA_HOME"
    else
      echo "ERROR: No Java 17 or Java 11 found under $JAVA_BASE"
      exit 1
    fi
  fi

  # Export JAVA_HOME
  export JAVA_HOME="$JAVA_HOME"

  echo "Printing JAVA_HOME = $JAVA_HOME"
}

# This function works for reading root yaml config key, does not work for nested key. This is needed only to read
# zk url which is at the root level.
read_config_key() {
  local config=$1
  local key=$2
  grep -E "^${key}:" "$config" \
          | cut -d: -f2- \
          | tr -d ' "' \
          | xargs   # trims whitespace
}

# Extract and export zk url as environment variable
export_zk_url() {
  local config=$1
  d2ZkUri=$(read_config_key $config d2ZkUri)
  export D2_ZK=$d2ZkUri
  echo_with_time "Printing zk url: $D2_ZK"
}


# CLI main function
function main() {

  # Setup JAVA_HOME
  setup_java_home

  # Download and extract distribution (will skip if OPENHOUSE_CLI_HOME already set)
  download_and_extract

  CLI_ARGS=""
  KEYSTORE_PROVIDED="false"
  EXECUTING_IDENTITY_PROVIDED="false"
  # Default cluster for which the token will be generated
  CLUSTER="ltx1-holdem"

  # Parse user arguments
  while [ "$#" -gt 0 ]; do
    case "$1" in
      -h|--help)
        usage
        exit
        ;;
      -e|--executingIdentity)
        EXECUTING_IDENTITY_PROVIDED="true"
        CLI_ARGS="${CLI_ARGS} $1 $2"
        shift
        shift
        ;;
      -r|--realIdentity)
        CLI_ARGS="${CLI_ARGS} $1 '$2'"
        shift
        shift
        ;;
      -f|--tokenFile)
        # If user specifies cluster, use that
        CLI_ARGS="${CLI_ARGS} $1 $2"
        shift
        shift
        ;;
      -k|--keystore)
        KEYSTORE_PROVIDED="true"
        CLI_ARGS="${CLI_ARGS} $1 $2"
        shift
        shift
        ;;
      -c|--cluster)
        CLUSTER="$2"
        shift
        shift
        ;;
      -stacktrace)
        CLI_ARGS="${CLI_ARGS} $1"
        shift
        shift
        ;;
      --) # End argument parsing
        shift
        break
        ;;
      *)
        CLI_ARGS="${CLI_ARGS} '$1'"
        shift
        ;;
    esac
  done

  if [ ${EXECUTING_IDENTITY_PROVIDED} == "false" ]; then
    CURRENT_USER="${USER:-$(whoami)}"
    CLI_ARGS="${CLI_ARGS} -e ${CURRENT_USER}"
  fi

  # Set classpath
  if [ -z ${CLASSPATH} ]; then
    CLASSPATH="${OPENHOUSE_CLI_HOME}/lib/*:/export/apps/hadoop/site/etc/hadoop"
  else
    CLASSPATH="${OPENHOUSE_CLI_HOME}/lib/*:/export/apps/hadoop/site/etc/hadoop:${CLASSPATH:-}"
  fi
  echo_with_time "Printing classpath: $CLASSPATH"

  # set log4j config
  LOG4J_CONF="file:${OPENHOUSE_CLI_HOME}/conf/log4j.properties"
  JAVA_OPTS="${JAVA_OPTS} -Djava.net.preferIPv4Stack=true"

  # set openhouse cli config path
  if [[ ${CLUSTER} == "ltx1-holdem" || ${CLUSTER} == "lva1-war" || ${CLUSTER} == "ltx1-lasso" || ${CLUSTER} == "ltx1-yugioh" ]]; then
    export OPENHOUSE_CLI_CONFIG_PATH="${OPENHOUSE_CLI_HOME}/conf/${CLUSTER}/ksudo_prod.yaml"
    export_zk_url $OPENHOUSE_CLI_CONFIG_PATH
  elif [[ ${CLUSTER} == "ltx1-faro" ]]; then
    export OPENHOUSE_CLI_CONFIG_PATH="${OPENHOUSE_CLI_HOME}/conf/${CLUSTER}/ksudo_ei.yaml"
    export_zk_url $OPENHOUSE_CLI_CONFIG_PATH
  else
    echo_with_time "Cluster: $CLUSTER is invalid. Specify a valid cluster from (ltx1-faro, ltx1-holdem, lva1-war, ltx1-lasso, ltx1-yugioh)"
    exit 1
  fi


  CMD=""
  if [ ${KEYSTORE_PROVIDED} == "true" ]; then
    CMD="$JAVA_HOME/bin/java ${JAVA_OPTS} -cp ${CLASSPATH} com.linkedin.openhouse.authentication.token.cli.OpenhouseTokenCli ${CLI_ARGS}"
  else
    CURRENT_USER="${USER:-$(whoami)}"
    CMD="$JAVA_HOME/bin/java ${JAVA_OPTS} -cp ${CLASSPATH} com.linkedin.openhouse.authentication.token.cli.OpenhouseTokenCli ${CLI_ARGS} --ldap ${CURRENT_USER}"
  fi
  echo_with_time "Executing command: ${CMD}"
  eval "${CMD}"
}

main "$@"
