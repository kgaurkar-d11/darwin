import requests
from typing import Dict, Any, List, Optional

from darwin_fs.config import *
from darwin_fs.model.data_response import DataResponse
from darwin_fs.model.read_features_request import ReadFeaturesRequest
from darwin_fs.model.read_features_response import ReadFeaturesResponse
from darwin_fs.model.write_features_request import WriteFeaturesRequest
from darwin_fs.model.write_features_response import WriteFeaturesResponse
from darwin_fs.util.exception_utils import parse_api_exception


def feature_group_write_features(write_features_request: WriteFeaturesRequest) -> WriteFeaturesResponse:
  url = DARWIN_OFS_V2_WRITER_HOST + DARWIN_OFS_V2_FG_WRITE_V2_ENDPOINT

  response = requests.request("POST", url, json=write_features_request.to_dict())
  if response.status_code != 200:
    parse_api_exception(response)

  return WriteFeaturesResponse.from_dict(DataResponse.from_dict(response.json()).data)


def feature_group_write_features_v1(write_request: Dict[str, Any]) -> Dict[str, Any]:
  """Write features using v1 format (legacy)."""
  url = DARWIN_OFS_V2_WRITER_HOST + DARWIN_OFS_V2_FG_WRITE_V1_ENDPOINT

  response = requests.request("POST", url, json=write_request)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def feature_group_read_features(read_features_request: ReadFeaturesRequest) -> ReadFeaturesResponse:
  url = DARWIN_OFS_V2_HOST + DARWIN_OFS_V2_FG_READ_ENDPOINT

  response = requests.request("GET", url, json=read_features_request.to_dict())
  if response.status_code != 200:
    parse_api_exception(response)

  return ReadFeaturesResponse.from_dict(DataResponse.from_dict(response.json()).data)


def feature_group_multi_read_features(read_requests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
  """Multi-read features from multiple feature groups."""
  url = DARWIN_OFS_V2_HOST + DARWIN_OFS_V2_FG_MULTI_READ_ENDPOINT
  body = {"requests": read_requests}

  response = requests.request("GET", url, json=body)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data


def feature_group_read_partition(read_partition_request: Dict[str, Any]) -> Dict[str, Any]:
  """Read feature partition."""
  url = DARWIN_OFS_V2_HOST + "/feature-group/read-partition"

  response = requests.request("GET", url, json=read_partition_request)
  if response.status_code != 200:
    parse_api_exception(response)

  return DataResponse.from_dict(response.json()).data
