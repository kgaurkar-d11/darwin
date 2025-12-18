import os

USER_ENV = os.environ.get("ENV", 'prod')

# Darwin local environment support
if USER_ENV == "darwin-local":
  DARWIN_OFS_V2_HOST = os.environ.get("OFS_V2_HOST", "http://localhost/feature-store")
  DARWIN_OFS_V2_ADMIN_HOST = os.environ.get("OFS_V2_ADMIN_HOST", "http://localhost/feature-store-admin")
  DARWIN_OFS_V2_WRITER_HOST = os.environ.get("OFS_V2_WRITER_HOST", "http://localhost/feature-store")
  DARWIN_OFS_V2_KAFKA_HOST = os.environ.get("OFS_V2_KAFKA_HOST", "localhost:9092")
elif USER_ENV == "uat":
  os.environ['TEAM_SUFFIX'] = '-uat'
  TEAM_SUFFIX = os.environ.get('TEAM_SUFFIX', '')
  VPC_SUFFIX = os.environ.get('VPC_SUFFIX', '')
  DARWIN_OFS_V2_HOST = "http://darwin-ofs-v2" + TEAM_SUFFIX + ".dream11" + VPC_SUFFIX + ".local"
  DARWIN_OFS_V2_ADMIN_HOST = "http://darwin-ofs-v2-admin" + TEAM_SUFFIX + ".dream11" + VPC_SUFFIX + ".local"
  DARWIN_OFS_V2_WRITER_HOST = DARWIN_OFS_V2_HOST
  DARWIN_OFS_V2_KAFKA_HOST = "darwin-ofs-v2-kafka" + TEAM_SUFFIX + ".dream11" + VPC_SUFFIX + ".local"
else:
  TEAM_SUFFIX = os.environ.get('TEAM_SUFFIX', '')
  VPC_SUFFIX = os.environ.get('VPC_SUFFIX', '')
  DARWIN_OFS_V2_HOST = "http://darwin-ofs-v2" + TEAM_SUFFIX + ".dream11" + VPC_SUFFIX + ".local"
  DARWIN_OFS_V2_ADMIN_HOST = "http://darwin-ofs-v2-admin" + TEAM_SUFFIX + ".dream11" + VPC_SUFFIX + ".local"
  DARWIN_OFS_V2_WRITER_HOST = "http://darwin-ofs-v2-writer" + TEAM_SUFFIX + ".dream11" + VPC_SUFFIX + ".local"
  DARWIN_OFS_V2_KAFKA_HOST = "darwin-ofs-v2-kafka" + TEAM_SUFFIX + ".dream11" + VPC_SUFFIX + ".local"

SDK_ENV = os.environ.get("sdk.python.environment", '')
if SDK_ENV == "test":
  DARWIN_OFS_V2_HOST = os.environ.get("ofs.server.host")
  DARWIN_OFS_V2_ADMIN_HOST = os.environ.get("ofs.admin.host")
  DARWIN_OFS_V2_WRITER_HOST = os.environ.get("ofs.server.host")
  DARWIN_OFS_V2_KAFKA_HOST = os.environ.get("ofs.kafka.host")

DARWIN_OFS_V2_ADMIN_ENTITY_ENDPOINT = "/entity"
DARWIN_OFS_V2_ADMIN_ENTITY_METADATA_ENDPOINT = "/entity/metadata"
DARWIN_OFS_V2_ADMIN_FG_ENDPOINT = "/feature-group"
DARWIN_OFS_V2_ADMIN_FG_METADATA_ENDPOINT = "/feature-group/metadata"
DARWIN_OFS_V2_ADMIN_FG_SCHEMA_ENDPOINT = "/feature-group/schema"
DARWIN_OFS_V2_ADMIN_FG_LATEST_VERSION_ENDPOINT = "/feature-group/version"

DARWIN_OFS_V2_FG_WRITE_V1_ENDPOINT = "/feature-group/write-features-v1"
DARWIN_OFS_V2_FG_WRITE_V2_ENDPOINT = "/feature-group/write-features-v2"
DARWIN_OFS_V2_FG_READ_ENDPOINT = "/feature-group/read-features"
DARWIN_OFS_V2_FG_MULTI_READ_ENDPOINT = "/feature-group/multi-read-features"

DARWIN_OFS_V2_OFFLINE_STORE_BASE_S3_PATH = os.environ.get(
  "OFS_V2_OFFLINE_STORE_PATH",
  f"s3a://darwin-ofs-v2-tables{os.environ.get('TEAM_SUFFIX', '')}/"
)

# Print config info for non-prod environments
if USER_ENV not in ('prod', 'darwin-local') and (os.environ.get('TEAM_SUFFIX', '') != '' or os.environ.get('VPC_SUFFIX', '') != ''):
  print(f"not pointing to prod\nTEAM_SUFFIX is set to {os.environ.get('TEAM_SUFFIX', '')}\nVPC_SUFFIX is set to {os.environ.get('VPC_SUFFIX', '')}")
  print(f"using configs:")
  print(f"\tofs_host: {DARWIN_OFS_V2_HOST}")
  print(f"\tofs_writer_host: {DARWIN_OFS_V2_WRITER_HOST}")
  print(f"\tofs_admin_host: {DARWIN_OFS_V2_ADMIN_HOST}")
  print(f"\tofs_kafka_host: {DARWIN_OFS_V2_KAFKA_HOST}")
