"""Feature Store CLI commands for darwin-cli."""

from typing import Optional

import typer
import yaml
from loguru import logger

from darwin_cli.utils.utils import _run_sync, to_dict, load_yaml_file
import darwin_fs.client as fs_client
import darwin_fs.service.ofs_v2_admin_service as admin_service
import darwin_fs.service.ofs_v2_service as ofs_service
from darwin_fs.model.create_entity_request import CreateEntityRequest
from darwin_fs.model.entity import Entity
from darwin_fs.model.feature_column import FeatureColumn
from darwin_fs.model.create_feature_group_request import CreateFeatureGroupRequest
from darwin_fs.model.feature_group import FeatureGroup
from darwin_fs.model.read_features_request import ReadFeaturesRequest
from darwin_fs.model.primary_keys import PrimaryKeys
from darwin_fs.model.write_features_request import WriteFeaturesRequest
from darwin_fs.model.write_features import WriteFeatures
from darwin_fs.constant.constants import DataType, FeatureGroupType


app = typer.Typer(
    name="feature-store",
    help="Feature Store operations - entities, feature groups, tenants, and features",
    no_args_is_help=True,
)

entity_app = typer.Typer(help="Entity operations")
fg_app = typer.Typer(help="Feature group operations")
tenant_app = typer.Typer(help="Tenant operations")
features_app = typer.Typer(help="Feature read/write operations")
run_data_app = typer.Typer(help="Run data operations")
search_app = typer.Typer(help="Search operations")

app.add_typer(entity_app, name="entity")
app.add_typer(fg_app, name="feature-group")
app.add_typer(tenant_app, name="tenant")
app.add_typer(features_app, name="features")
app.add_typer(run_data_app, name="run-data")
app.add_typer(search_app, name="search")






def _build_entity_from_config(config: dict) -> tuple[Entity, CreateEntityRequest]:
    """Build Entity and CreateEntityRequest from config dict."""
    entity_config = config.get("entity", {})
    features = [
        FeatureColumn(
            name=f["name"],
            type=DataType(f["type"]),
            description=f.get("description", ""),
            tags=f.get("tags", [])
        )
        for f in entity_config.get("features", [])
    ]
    
    entity = Entity(
        table_name=entity_config["table_name"],
        primary_keys=entity_config["primary_keys"],
        features=features,
        ttl=entity_config.get("ttl", 0)
    )
    
    request = CreateEntityRequest(
        entity=entity,
        owner=config.get("owner", ""),
        tags=config.get("tags", []),
        description=config.get("description", "")
    )
    return entity, request


def _build_feature_group_from_config(config: dict) -> tuple[FeatureGroup, CreateFeatureGroupRequest]:
    """Build FeatureGroup and CreateFeatureGroupRequest from config dict."""
    fg_config = config.get("feature_group", {})
    features = [
        FeatureColumn(
            name=f["name"],
            type=DataType(f["type"]),
            description=f.get("description", ""),
            tags=f.get("tags", [])
        )
        for f in fg_config.get("features", [])
    ]
    
    fg_type_str = fg_config.get("feature_group_type", "ONLINE")
    fg_type = FeatureGroupType.ONLINE if fg_type_str == "ONLINE" else FeatureGroupType.OFFLINE
    
    feature_group = FeatureGroup(
        feature_group_name=fg_config["feature_group_name"],
        entity_name=fg_config["entity_name"],
        features=features,
        feature_group_type=fg_type,
        version_enabled=fg_config.get("version_enabled", True)
    )
    
    request = CreateFeatureGroupRequest(
        feature_group=feature_group,
        owner=config.get("owner", ""),
        tags=config.get("tags", []),
        description=config.get("description", "")
    )
    return feature_group, request


# Entity Operations

@entity_app.command("create")
def entity_create(
    file: str = typer.Option(..., "--file", help="YAML config file for entity"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Create an entity from YAML configuration."""
    config = load_yaml_file(file)
    try:
        _, request = _build_entity_from_config(config)
        result = _run_sync(fs_client.create_entity, request, retries=retries)
        logger.info("Entity created successfully")
        typer.echo(result)
    except Exception as e:
        logger.error("Error creating entity: {}", e)
        raise typer.Exit(1)


@entity_app.command("register")
def entity_register(
    file: str = typer.Option(..., "--file", help="YAML config file for entity"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Register existing entity metadata (doesn't create Cassandra table)."""
    config = load_yaml_file(file)
    try:
        _, request = _build_entity_from_config(config)
        result = _run_sync(admin_service.register_entity, request, retries=retries)
        logger.info("Entity registered successfully")
        typer.echo(result)
    except Exception as e:
        logger.error("Error registering entity: {}", e)
        raise typer.Exit(1)


@entity_app.command("get")
def entity_get(
    name: str = typer.Option(..., "--name", help="Entity name"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get entity by name."""
    try:
        result = _run_sync(admin_service.get_entity, name, retries=retries)
        logger.info("Retrieved entity: {}", name)
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error getting entity: {}", e)
        raise typer.Exit(1)


@entity_app.command("update")
def entity_update(
    name: str = typer.Option(..., "--name", help="Entity name"),
    file: str = typer.Option(..., "--file", help="YAML file with update data"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Update entity metadata."""
    config = load_yaml_file(file)
    try:
        result = _run_sync(admin_service.update_entity, name, config, retries=retries)
        logger.info("Entity '{}' updated successfully", name)
        typer.echo(result)
    except Exception as e:
        logger.error("Error updating entity: {}", e)
        raise typer.Exit(1)


@entity_app.command("get-metadata")
def entity_get_metadata(
    name: str = typer.Option(..., "--name", help="Entity name"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get entity metadata."""
    try:
        result = _run_sync(admin_service.get_entity_metadata, name, retries=retries)
        logger.info("Retrieved entity metadata: {}", name)
        typer.echo(yaml.dump(to_dict(result), default_flow_style=False))
    except Exception as e:
        logger.error("Error getting entity metadata: {}", e)
        raise typer.Exit(1)


@entity_app.command("list")
def entity_list(
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """List all entities."""
    try:
        result = _run_sync(admin_service.list_all_entities, False, retries=retries)
        logger.info("Retrieved all entities")
        typer.echo(result)
    except Exception as e:
        logger.error("Error listing entities: {}", e)
        raise typer.Exit(1)


@entity_app.command("list-updated")
def entity_list_updated(
    since: int = typer.Option(..., "--since", help="Unix timestamp to filter entities updated since"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """List entities updated since timestamp."""
    try:
        result = _run_sync(admin_service.list_updated_entities, since, False, retries=retries)
        logger.info("Retrieved entities updated since {}", since)
        typer.echo(result)
    except Exception as e:
        logger.error("Error listing updated entities: {}", e)
        raise typer.Exit(1)


# Feature Group Operations

@fg_app.command("create")
def fg_create(
    file: str = typer.Option(..., "--file", help="YAML config file for feature group"),
    upgrade: bool = typer.Option(False, "--upgrade", help="Upgrade version if exists"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Create a feature group from YAML configuration."""
    config = load_yaml_file(file)
    try:
        _, request = _build_feature_group_from_config(config)
        result = _run_sync(fs_client.create_feature_group, request, upgrade, retries=retries)
        logger.info("Feature group created successfully")
        typer.echo(result)
    except Exception as e:
        logger.error("Error creating feature group: {}", e)
        raise typer.Exit(1)


@fg_app.command("register")
def fg_register(
    file: str = typer.Option(..., "--file", help="YAML config file for feature group"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Register existing feature group metadata."""
    config = load_yaml_file(file)
    try:
        _, request = _build_feature_group_from_config(config)
        result = _run_sync(admin_service.register_feature_group, request, False, retries=retries)
        logger.info("Feature group registered successfully")
        typer.echo(result)
    except Exception as e:
        logger.error("Error registering feature group: {}", e)
        raise typer.Exit(1)


@fg_app.command("get")
def fg_get(
    name: str = typer.Option(..., "--name", help="Feature group name"),
    version: Optional[str] = typer.Option(None, "--version", help="Feature group version"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get feature group by name and optional version."""
    try:
        result = _run_sync(admin_service.get_feature_group, name, version, retries=retries)
        logger.info("Retrieved feature group: {}", name)
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error getting feature group: {}", e)
        raise typer.Exit(1)


@fg_app.command("update")
def fg_update(
    name: str = typer.Option(..., "--name", help="Feature group name"),
    version: str = typer.Option(..., "--version", help="Feature group version"),
    file: str = typer.Option(..., "--file", help="YAML file with update data"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Update feature group metadata."""
    config = load_yaml_file(file)
    try:
        result = _run_sync(admin_service.update_feature_group, name, version, config, retries=retries)
        logger.info("Feature group '{}' version '{}' updated successfully", name, version)
        typer.echo(result)
    except Exception as e:
        logger.error("Error updating feature group: {}", e)
        raise typer.Exit(1)


@fg_app.command("schema")
def fg_schema(
    name: str = typer.Option(..., "--name", help="Feature group name"),
    version: Optional[str] = typer.Option(None, "--version", help="Feature group version"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get feature group schema."""
    try:
        result = _run_sync(fs_client.get_feature_group_schema, name, version, retries=retries)
        logger.info("Retrieved schema for feature group: {}", name)
        typer.echo(yaml.dump(to_dict(result), default_flow_style=False))
    except Exception as e:
        logger.error("Error getting feature group schema: {}", e)
        raise typer.Exit(1)


@fg_app.command("metadata")
def fg_metadata(
    name: str = typer.Option(..., "--name", help="Feature group name"),
    version: Optional[str] = typer.Option(None, "--version", help="Feature group version"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get feature group metadata."""
    try:
        result = _run_sync(admin_service.get_feature_group_metadata, name, version, retries=retries)
        logger.info("Retrieved metadata for feature group: {}", name)
        typer.echo(yaml.dump(to_dict(result), default_flow_style=False))
    except Exception as e:
        logger.error("Error getting feature group metadata: {}", e)
        raise typer.Exit(1)


@fg_app.command("version")
def fg_version(
    name: str = typer.Option(..., "--name", help="Feature group name"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get latest version of a feature group."""
    try:
        result = _run_sync(fs_client.get_latest_feature_group_version, name, retries=retries)
        logger.info("Retrieved latest version for feature group: {}", name)
        typer.echo(yaml.dump(to_dict(result), default_flow_style=False))
    except Exception as e:
        logger.error("Error getting feature group version: {}", e)
        raise typer.Exit(1)


@fg_app.command("list")
def fg_list(
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """List all feature groups."""
    try:
        result = _run_sync(admin_service.list_all_feature_groups, False, retries=retries)
        logger.info("Retrieved all feature groups")
        typer.echo(result)
    except Exception as e:
        logger.error("Error listing feature groups: {}", e)
        raise typer.Exit(1)


@fg_app.command("list-versions")
def fg_list_versions(
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """List all feature group versions."""
    try:
        result = _run_sync(admin_service.list_all_feature_group_versions, False, retries=retries)
        logger.info("Retrieved all feature group versions")
        typer.echo(result)
    except Exception as e:
        logger.error("Error listing feature group versions: {}", e)
        raise typer.Exit(1)


@fg_app.command("update-ttl")
def fg_update_ttl(
    name: str = typer.Option(..., "--name", help="Feature group name"),
    version: str = typer.Option(..., "--version", help="Feature group version"),
    ttl: int = typer.Option(..., "--ttl", help="New TTL value in seconds"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Update TTL for a feature group."""
    try:
        result = _run_sync(admin_service.update_feature_group_ttl, name, version, ttl, retries=retries)
        logger.info("Updated TTL for feature group '{}' version '{}' to {} seconds", name, version, ttl)
        typer.echo(result)
    except Exception as e:
        logger.error("Error updating feature group TTL: {}", e)
        raise typer.Exit(1)


# Tenant Operations

@tenant_app.command("add")
def tenant_add(
    file: str = typer.Option(..., "--file", help="YAML config file for tenant"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Add tenant to feature group."""
    config = load_yaml_file(file)
    try:
        feature_group_name = config["feature_group_name"]
        tenant_config = config["tenant_config"]
        result = _run_sync(admin_service.add_tenant, feature_group_name, tenant_config, retries=retries)
        logger.info("Tenant added to feature group '{}'", feature_group_name)
        typer.echo(result)
    except Exception as e:
        logger.error("Error adding tenant: {}", e)
        raise typer.Exit(1)


@tenant_app.command("get")
def tenant_get(
    feature_group: str = typer.Option(..., "--feature-group", help="Feature group name"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get tenant for feature group."""
    try:
        result = _run_sync(admin_service.get_tenant, feature_group, retries=retries)
        logger.info("Retrieved tenant for feature group: {}", feature_group)
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error getting tenant: {}", e)
        raise typer.Exit(1)


@tenant_app.command("topic")
def tenant_topic(
    feature_group: str = typer.Option(..., "--feature-group", help="Feature group name"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get tenant Kafka topic for feature group."""
    try:
        result = _run_sync(admin_service.get_tenant_topic, feature_group, retries=retries)
        logger.info("Retrieved tenant topic for feature group: {}", feature_group)
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error getting tenant topic: {}", e)
        raise typer.Exit(1)


@tenant_app.command("list")
def tenant_list(
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """List all tenants."""
    try:
        result = _run_sync(admin_service.list_all_tenants, retries=retries)
        logger.info("Retrieved all tenants")
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error listing tenants: {}", e)
        raise typer.Exit(1)


@tenant_app.command("feature-groups")
def tenant_feature_groups(
    tenant_name: str = typer.Option(..., "--tenant-name", help="Tenant name"),
    tenant_type: str = typer.Option("ALL", "--tenant-type", help="Tenant type (READER|WRITER|CONSUMER|ALL)"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get feature groups for a tenant."""
    try:
        result = _run_sync(admin_service.get_feature_groups_for_tenant, tenant_name, tenant_type, retries=retries)
        logger.info("Retrieved feature groups for tenant: {}", tenant_name)
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error getting feature groups for tenant: {}", e)
        raise typer.Exit(1)


@tenant_app.command("update-all")
def tenant_update_all(
    file: str = typer.Option(..., "--file", help="YAML file with tenant update data"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Update all tenants."""
    config = load_yaml_file(file)
    try:
        tenant_type = config["tenant_type"]
        current_tenant = config["current_tenant"]
        new_tenant = config["new_tenant"]
        result = _run_sync(admin_service.update_all_tenants, tenant_type, current_tenant, new_tenant, retries=retries)
        logger.info("Updated all tenants from '{}' to '{}'", current_tenant, new_tenant)
        typer.echo(result)
    except Exception as e:
        logger.error("Error updating all tenants: {}", e)
        raise typer.Exit(1)


# Feature Read/Write Operations

@features_app.command("read")
def features_read(
    file: str = typer.Option(..., "--file", help="YAML file with read request"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Read features from a feature group."""
    config = load_yaml_file(file)
    try:
        pk_config = config.get("primary_keys", {})
        primary_keys = PrimaryKeys(
            names=pk_config.get("names", []),
            values=pk_config.get("values", [])
        )
        
        request = ReadFeaturesRequest(
            feature_group_name=config["feature_group_name"],
            feature_columns=config.get("feature_columns", []),
            primary_keys=primary_keys,
            feature_group_version=config.get("feature_group_version")
        )
        
        result = _run_sync(fs_client.read_features, request, retries=retries)
        logger.info("Read features from feature group: {}", config["feature_group_name"])
        typer.echo(yaml.dump(to_dict(result), default_flow_style=False))
    except Exception as e:
        logger.error("Error reading features: {}", e)
        raise typer.Exit(1)


@features_app.command("multi-read")
def features_multi_read(
    file: str = typer.Option(..., "--file", help="YAML file with multi-read request"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Multi-read features from multiple feature groups."""
    config = load_yaml_file(file)
    try:
        requests = config.get("requests", [])
        formatted_requests = [
            {
                "featureGroupName": req["feature_group_name"],
                "featureGroupVersion": req.get("feature_group_version"),
                "featureColumns": req.get("feature_columns", []),
                "primaryKeys": {
                    "names": req.get("primary_keys", {}).get("names", []),
                    "values": req.get("primary_keys", {}).get("values", [])
                }
            }
            for req in requests
        ]
        
        result = _run_sync(ofs_service.feature_group_multi_read_features, formatted_requests, retries=retries)
        logger.info("Multi-read features from {} feature groups", len(requests))
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error multi-reading features: {}", e)
        raise typer.Exit(1)


@features_app.command("read-partition")
def features_read_partition(
    file: str = typer.Option(..., "--file", help="YAML file with partition read request"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Read feature partition."""
    config = load_yaml_file(file)
    try:
        request = {
            "featureGroupName": config["feature_group_name"],
            "featureGroupVersion": config.get("feature_group_version"),
            "featureColumns": config.get("feature_columns", []),
            "partitionKey": {
                "names": config.get("partitionKey", {}).get("names", []),
                "values": config.get("partitionKey", {}).get("values", [])
            }
        }
        
        result = _run_sync(ofs_service.feature_group_read_partition, request, retries=retries)
        logger.info("Read partition from feature group: {}", config["feature_group_name"])
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error reading partition: {}", e)
        raise typer.Exit(1)


@features_app.command("write")
def features_write(
    file: str = typer.Option(..., "--file", help="YAML file with write request"),
    format: str = typer.Option("v2", "--format", help="Write format (v1|v2)"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Write features to a feature group."""
    config = load_yaml_file(file)
    try:
        if format == "v1":
            result = _run_sync(ofs_service.feature_group_write_features_v1, config, retries=retries)
        else:
            features_config = config.get("features", {})
            features = WriteFeatures(
                names=features_config.get("names", []),
                values=features_config.get("values", [])
            )
            
            request = WriteFeaturesRequest(
                feature_group_name=config["feature_group_name"],
                features=features,
                feature_group_version=config.get("feature_group_version")
            )
            
            result = _run_sync(fs_client.write_features_sync, request, retries=retries)
        
        logger.info("Wrote features to feature group: {}", config["feature_group_name"])
        typer.echo(yaml.dump(to_dict(result), default_flow_style=False))
    except Exception as e:
        logger.error("Error writing features: {}", e)
        raise typer.Exit(1)


# Run Data Operations

@run_data_app.command("add")
def run_data_add(
    file: str = typer.Option(..., "--file", help="YAML file with run data"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Add run data for a feature group."""
    config = load_yaml_file(file)
    try:
        result = _run_sync(
            admin_service.add_run_data,
            config["name"],
            config["version"],
            config["run_id"],
            config.get("time_taken"),
            config.get("count"),
            config.get("status"),
            config.get("error_message"),
            config.get("sample_data"),
            retries=retries
        )
        logger.info("Added run data for feature group: {}", config["name"])
        typer.echo(result)
    except Exception as e:
        logger.error("Error adding run data: {}", e)
        raise typer.Exit(1)


@run_data_app.command("get")
def run_data_get(
    name: str = typer.Option(..., "--name", help="Feature group name"),
    version: Optional[str] = typer.Option(None, "--version", help="Feature group version"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get run data for a feature group."""
    try:
        result = _run_sync(admin_service.get_run_data, name, version, retries=retries)
        logger.info("Retrieved run data for feature group: {}", name)
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error getting run data: {}", e)
        raise typer.Exit(1)


# Search Operations

@search_app.command("owners")
def search_owners(
    type: str = typer.Option(..., "--type", help="Type (feature-group|entity)"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get all distinct owners."""
    try:
        if type == "feature-group":
            result = _run_sync(admin_service.get_feature_group_owners, retries=retries)
            logger.info("Retrieved all feature group owners")
        elif type == "entity":
            result = _run_sync(admin_service.get_entity_owners, retries=retries)
            logger.info("Retrieved all entity owners")
        else:
            logger.error("Invalid type: {}. Use 'feature-group' or 'entity'", type)
            raise typer.Exit(1)
        
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error getting owners: {}", e)
        raise typer.Exit(1)


@search_app.command("tags")
def search_tags(
    type: str = typer.Option(..., "--type", help="Type (feature-group|entity)"),
    retries: int = typer.Option(1, "--retries", help="Number of retries"),
):
    """Get all distinct tags."""
    try:
        if type == "feature-group":
            result = _run_sync(admin_service.get_feature_group_tags, retries=retries)
            logger.info("Retrieved all feature group tags")
        elif type == "entity":
            result = _run_sync(admin_service.get_entity_tags, retries=retries)
            logger.info("Retrieved all entity tags")
        else:
            logger.error("Invalid type: {}. Use 'feature-group' or 'entity'", type)
            raise typer.Exit(1)
        
        typer.echo(yaml.dump(result, default_flow_style=False))
    except Exception as e:
        logger.error("Error getting tags: {}", e)
        raise typer.Exit(1)
