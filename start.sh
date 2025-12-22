#!/bin/sh
set -e

# Check for init configuration
ENABLED_SERVICES_FILE=".setup/enabled-services.yaml"
if [ ! -f "$ENABLED_SERVICES_FILE" ]; then
    echo "❌ No configuration found at $ENABLED_SERVICES_FILE"
    echo "   Please run ./init.sh first to configure which services to enable."
    exit 1
fi
echo "✅ Found configuration: $ENABLED_SERVICES_FILE"

# ============================================================================
# CLI TOOLS SETUP
# ============================================================================
# Check if darwin-cli is enabled and install if needed
DARWIN_CLI_ENABLED=$(yq eval '.cli-tools.darwin-cli // false' "$ENABLED_SERVICES_FILE" 2>/dev/null || echo "false")

if [ "$DARWIN_CLI_ENABLED" = "true" ]; then
  echo ""
  echo "📦 Setting up darwin-cli..."
  
  DARWIN_CLI_PATH="darwin-cli"
  if [ ! -d "$DARWIN_CLI_PATH" ]; then
    echo "   ⚠️  darwin-cli directory not found at $DARWIN_CLI_PATH, skipping..."
  else
    VENV_PATH=".venv"
    
    # Create venv if it doesn't exist
    if [ ! -d "$VENV_PATH" ]; then
      echo "   Creating virtual environment..."
      python3.9 -m venv "$VENV_PATH"
    fi

    # Install darwin-cli
    echo "   Installing darwin-cli package..."
    (
      source "$VENV_PATH/bin/activate" && cd "$DARWIN_CLI_PATH" && python setup.py sdist && pip install --upgrade pip && pip install dist/darwin-cli-1.0.0.tar.gz --force-reinstall
    )

    if [ $? -eq 0 ]; then
      echo "   ✅ darwin-cli installed successfully"
      echo "   To use: source $VENV_PATH/bin/activate && darwin --help"
    else
      echo "   ❌ Failed to install darwin-cli"
    fi
  fi
  echo ""
fi

# Source the config.env file
set -o allexport
. config.env
set +o allexport

echo "🔧 Setting up KUBECONFIG: $KUBECONFIG"

# Verify cluster connectivity
if kubectl version >/dev/null 2>&1; then
  echo "✅ Cluster is accessible"
  kubectl get nodes
else
  echo "❌ Cluster is not reachable. Please run setup.sh first."
  exit 1
fi

echo "⚙️  Setting up Kubernetes dependencies..."
./k8s-setup.sh

echo "🚀 Starting Darwin Platform deployment..."

# ============================================================================
# BUILD HELM OVERRIDES FROM CONFIG
# ============================================================================
echo "📋 Reading service configuration..."

HELM_OVERRIDES=""

# Function to map application name to helm path
get_helm_path() {
  local app_name="$1"
  case "$app_name" in
    "darwin-ofs-v2") echo "services.services.feature-store.enabled" ;;
    "darwin-ofs-v2-admin") echo "services.services.feature-store-admin.enabled" ;;
    "darwin-ofs-v2-consumer") echo "services.services.feature-store-consumer.enabled" ;;
    "darwin-mlflow") echo "services.services.mlflow-lib.enabled" ;;
    "darwin-mlflow-app") echo "services.services.mlflow-app.enabled" ;;
    "chronos") echo "services.services.chronos.enabled" ;;
    "chronos-consumer") echo "services.services.chronos-consumer.enabled" ;;
    "darwin-compute") echo "services.services.compute.enabled" ;;
    "darwin-cluster-manager") echo "services.services.cluster-manager.enabled" ;;
    "darwin-workspace") echo "services.services.workspace.enabled" ;;
    "ml-serve-app") echo "services.services.ml-serve-app.enabled" ;;
    "artifact-builder") echo "services.services.artifact-builder.enabled" ;;
    "darwin-catalog") echo "services.services.catalog.enabled" ;;
    *) echo "" ;;
  esac
}

# Read applications from config and build --set flags
echo "   Processing applications..."
for app_name in $(yq eval '.applications | keys | .[]' "$ENABLED_SERVICES_FILE"); do
  enabled=$(yq eval ".applications.\"$app_name\"" "$ENABLED_SERVICES_FILE")
  helm_path=$(get_helm_path "$app_name")
  
  if [ -n "$helm_path" ]; then
    HELM_OVERRIDES="$HELM_OVERRIDES --set $helm_path=$enabled"
    echo "     $app_name -> $helm_path=$enabled"
  fi
done

# Read datastores from config and build --set flags (direct mapping)
echo "   Processing datastores..."
for ds_name in $(yq eval '.datastores | keys | .[]' "$ENABLED_SERVICES_FILE"); do
  enabled=$(yq eval ".datastores.\"$ds_name\"" "$ENABLED_SERVICES_FILE")
  
  # Skip busybox - it's not a helm-managed datastore
  if [ "$ds_name" = "busybox" ]; then
    continue
  fi
  
  helm_path="datastores.$ds_name.enabled"
  HELM_OVERRIDES="$HELM_OVERRIDES --set $helm_path=$enabled"
  echo "     $ds_name -> $helm_path=$enabled"
done

echo ""
echo "📦 Installing Darwin Platform with configuration overrides..."

# Install Darwin Platform umbrella chart with overrides
helm upgrade --install darwin ./helm/darwin \
  --namespace darwin \
  --create-namespace \
  --wait \
  --timeout 600s \
  $HELM_OVERRIDES

echo "✅ Deployment completed!"

# Show darwin-cli activation reminder if it was installed
DARWIN_CLI_ENABLED=$(yq eval '.cli-tools.darwin-cli // false' "$ENABLED_SERVICES_FILE" 2>/dev/null || echo "false")
if [ "$DARWIN_CLI_ENABLED" = "true" ]; then
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "📦 To use darwin-cli, activate the virtual environment:"
  echo ""
  echo "   source .venv/bin/activate"
  echo "   darwin --help"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
fi
