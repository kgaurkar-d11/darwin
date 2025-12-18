import requests
from typing import Dict, Any, List, Optional

from darwin_fs.config import *
from darwin_fs.constant.constants import State
from darwin_fs.model.create_entity_request import CreateEntityRequest
from darwin_fs.model.create_entity_response import CreateEntityResponse
from darwin_fs.model.create_feature_group_request import CreateFeatureGroupRequest
from darwin_fs.model.create_feature_group_response import CreateFeatureGroupResponse
from darwin_fs.model.data_response import DataResponse
from darwin_fs.model.entity import Entity
from darwin_fs.model.entity_metadata import EntityMetadata
from darwin_fs.model.feature_group import FeatureGroup
from darwin_fs.model.feature_group_metadata import FeatureGroupMetadata
from darwin_fs.model.feature_group_schema import FeatureGroupSchema
from darwin_fs.model.metadata_response import MetadataResponse
from darwin_fs.model.version_metadata import VersionMetadata
from darwin_fs.model.tenant_config import TenantConfig
from darwin_fs.util.exception_utils import parse_api_exception


def get_feature_group_schema(feature_group_name: str, feature_group_version: None) -> FeatureGroupSchema:
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_SCHEMA_ENDPOINT
  params = {"name": feature_group_name}
  if feature_group_version is not None:
    params["version"] = feature_group_version

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)

  return FeatureGroupSchema.from_dict(DataResponse.from_dict(response.json()).data)


def get_feature_group_metadata(feature_group_name: str, feature_group_version: None) -> FeatureGroupMetadata:
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_METADATA_ENDPOINT
  params = {"name": feature_group_name}
  if feature_group_version is not None:
    params["version"] = feature_group_version

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)
  metadata = MetadataResponse.from_dict(DataResponse.from_dict(response.json()).data).metadata
  return FeatureGroupMetadata.from_dict(metadata)


def get_entity_metadata(entity_name: str) -> EntityMetadata:
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_ENTITY_METADATA_ENDPOINT
  params = {"name": entity_name}

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)
  metadata = MetadataResponse.from_dict(DataResponse.from_dict(response.json()).data).metadata
  return EntityMetadata.from_dict(metadata)


def create_entity(create_entity_request: CreateEntityRequest) -> Entity:
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_ENTITY_ENDPOINT

  response = requests.request("POST", url, json=create_entity_request.to_dict())
  if response.status_code != 200:
    parse_api_exception(response)

  return CreateEntityResponse.from_dict(DataResponse.from_dict(response.json()).data).entity


def register_entity(create_entity_request: CreateEntityRequest) -> Entity:
  """Register existing entity metadata (doesn't create Cassandra table)."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_ENTITY_ENDPOINT + "/register"

  response = requests.request("POST", url, json=create_entity_request.to_dict())
  if response.status_code != 200:
    parse_api_exception(response)

  return CreateEntityResponse.from_dict(DataResponse.from_dict(response.json()).data).entity


def get_entity(entity_name: str) -> Dict[str, Any]:
  """Get entity by name."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_ENTITY_ENDPOINT
  params = {"name": entity_name}

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def update_entity(entity_name: str, update_data: Dict[str, Any]) -> Dict[str, Any]:
  """Update entity metadata (owner, tags, description)."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_ENTITY_ENDPOINT
  params = {"name": entity_name}

  response = requests.request("PUT", url, params=params, json=update_data)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def list_all_entities(compressed: bool = False) -> List[Dict[str, Any]]:
  """List all entities."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_ENTITY_ENDPOINT + "/all"
  headers = {"compressed-response": str(compressed).lower()}

  response = requests.request("GET", url, headers=headers)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def list_updated_entities(timestamp: int, compressed: bool = False) -> List[Dict[str, Any]]:
  """List entities updated since timestamp."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_ENTITY_ENDPOINT + "/all-updated"
  params = {"timestamp": timestamp}
  headers = {"compressed-response": str(compressed).lower()}

  response = requests.request("GET", url, params=params, headers=headers)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def create_feature_group(create_feature_group_request: CreateFeatureGroupRequest, upgrade: bool = False) -> (FeatureGroup, str):
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT

  headers = {}
  if upgrade:
    headers = {"upgrade": "true"}
  response = requests.request("POST", url, json=create_feature_group_request.to_dict(), headers=headers)
  if response.status_code != 200:
    parse_api_exception(response)
  create_response = CreateFeatureGroupResponse.from_dict(DataResponse.from_dict(response.json()).data)
  return (create_response.feature_group, create_response.version)


def register_feature_group(create_feature_group_request: CreateFeatureGroupRequest, upgrade: bool = False) -> (FeatureGroup, str):
  """Register existing feature group metadata."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/register"

  headers = {}
  if upgrade:
    headers = {"upgrade": "true"}
  response = requests.request("POST", url, json=create_feature_group_request.to_dict(), headers=headers)
  if response.status_code != 200:
    parse_api_exception(response)
  create_response = CreateFeatureGroupResponse.from_dict(DataResponse.from_dict(response.json()).data)
  return (create_response.feature_group, create_response.version)


def get_feature_group(feature_group_name: str, feature_group_version: Optional[str] = None) -> Dict[str, Any]:
  """Get feature group by name and optional version."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT
  params = {"name": feature_group_name}
  if feature_group_version is not None:
    params["version"] = feature_group_version

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def update_feature_group(feature_group_name: str, feature_group_version: str, update_data: Dict[str, Any]) -> Dict[str, Any]:
  """Update feature group metadata."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT
  params = {"name": feature_group_name, "version": feature_group_version}

  response = requests.request("PUT", url, params=params, json=update_data)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def get_feature_group_latest_version(feature_group_name: str) -> VersionMetadata:
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_LATEST_VERSION_ENDPOINT
  params = {"name": feature_group_name}

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)

  return VersionMetadata.from_dict(DataResponse.from_dict(response.json()).data)


def update_feature_group_state(state: State, feature_group_name: str, feature_group_version: None) -> None:
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT
  params = {"name": feature_group_name}
  if feature_group_version is not None:
    params["version"] = feature_group_version

  body = {
    "state": state
  }
  response = requests.request("PUT", url, params=params, json=body)
  if response.status_code != 200:
    parse_api_exception(response)


def list_all_feature_groups(compressed: bool = False) -> List[Dict[str, Any]]:
  """List all feature groups."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/all"
  headers = {"compressed-response": str(compressed).lower()}

  response = requests.request("GET", url, headers=headers)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def list_updated_feature_groups(timestamp: int, compressed: bool = False) -> List[Dict[str, Any]]:
  """List feature groups updated since timestamp."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/all-updated"
  params = {"timestamp": timestamp}
  headers = {"compressed-response": str(compressed).lower()}

  response = requests.request("GET", url, params=params, headers=headers)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def list_all_feature_group_versions(compressed: bool = False) -> List[Dict[str, Any]]:
  """List all feature group versions."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_LATEST_VERSION_ENDPOINT + "/all"
  headers = {"compressed-response": str(compressed).lower()}

  response = requests.request("GET", url, headers=headers)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def update_feature_group_ttl(feature_group_name: str, feature_group_version: str, ttl: int) -> Dict[str, Any]:
  """Update TTL for a feature group."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/ttl"
  body = {
    "name": feature_group_name,
    "version": feature_group_version,
    "ttl": ttl
  }

  response = requests.request("PUT", url, json=body)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


# Tenant Operations
def add_tenant(feature_group_name: str, tenant_config: Dict[str, str]) -> Dict[str, Any]:
  """Add tenant to feature group."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/tenant"
  body = {
    "featureGroupName": feature_group_name,
    "tenantConfig": tenant_config
  }

  response = requests.request("POST", url, json=body)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def get_tenant(feature_group_name: str) -> Dict[str, Any]:
  """Get tenant for feature group."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/tenant"
  params = {"feature-group-name": feature_group_name}

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def get_tenant_topic(feature_group_name: str) -> Dict[str, Any]:
  """Get tenant Kafka topic for feature group."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/topic"
  params = {"name": feature_group_name}

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def list_all_tenants() -> Dict[str, Any]:
  """List all tenants."""
  url = DARWIN_OFS_V2_ADMIN_HOST + "/tenant/list"

  response = requests.request("GET", url)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def get_feature_groups_for_tenant(tenant_name: str, tenant_type: str = "ALL") -> Dict[str, Any]:
  """Get feature groups for a tenant."""
  url = DARWIN_OFS_V2_ADMIN_HOST + "/tenant/feature-group/all"
  params = {"tenant-name": tenant_name, "tenant-type": tenant_type}

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def update_all_tenants(tenant_type: str, current_tenant: str, new_tenant: str) -> Dict[str, Any]:
  """Update all tenants."""
  url = DARWIN_OFS_V2_ADMIN_HOST + "/tenant/all"
  body = {
    "tenantType": tenant_type,
    "currentTenant": current_tenant,
    "newTenant": new_tenant
  }

  response = requests.request("PUT", url, json=body)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


# Run Data Operations
def add_run_data(name: str, version: str, run_id: str, time_taken: Optional[int] = None,
                 count: Optional[int] = None, status: Optional[str] = None,
                 error_message: Optional[str] = None, sample_data: Optional[List] = None) -> Dict[str, Any]:
  """Add run data for feature group."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/run-data"
  body = {
    "name": name,
    "version": version,
    "runId": run_id
  }
  if time_taken is not None:
    body["timeTaken"] = time_taken
  if count is not None:
    body["count"] = count
  if status is not None:
    body["status"] = status
  if error_message is not None:
    body["errorMessage"] = error_message
  if sample_data is not None:
    body["sampleData"] = sample_data

  response = requests.request("POST", url, json=body)
  if response.status_code != 200:
    parse_api_exception(response)

  return response.json()


def get_run_data(name: str, version: Optional[str] = None) -> List[Dict[str, Any]]:
  """Get run data for feature group."""
  url = DARWIN_OFS_V2_ADMIN_HOST + DARWIN_OFS_V2_ADMIN_FG_ENDPOINT + "/run-data"
  params = {"name": name}
  if version is not None:
    params["version"] = version

  response = requests.request("GET", url, params=params)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


# Search Operations
def get_feature_group_owners() -> Dict[str, Any]:
  """Get all distinct feature group owners."""
  url = DARWIN_OFS_V2_ADMIN_HOST + "/search/all-feature-group-owners"

  response = requests.request("GET", url)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def get_entity_owners() -> Dict[str, Any]:
  """Get all distinct entity owners."""
  url = DARWIN_OFS_V2_ADMIN_HOST + "/search/all-entity-owners"

  response = requests.request("GET", url)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def get_feature_group_tags() -> Dict[str, Any]:
  """Get all distinct feature group tags."""
  url = DARWIN_OFS_V2_ADMIN_HOST + "/search/all-feature-group-tags"

  response = requests.request("GET", url)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def get_entity_tags() -> Dict[str, Any]:
  """Get all distinct entity tags."""
  url = DARWIN_OFS_V2_ADMIN_HOST + "/search/all-entity-tags"

  response = requests.request("GET", url)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data
