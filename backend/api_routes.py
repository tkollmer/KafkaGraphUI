"""REST API routes for Kafka management operations."""

import logging
from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")


def _get_admin(request: Request):
    admin = getattr(request.app.state, "kafka_admin", None)
    if not admin:
        raise HTTPException(status_code=503, detail="Kafka admin not available")
    return admin


def _get_sampler(request: Request):
    sampler = getattr(request.app.state, "message_sampler", None)
    if not sampler:
        raise HTTPException(status_code=503, detail="Message sampler not available")
    return sampler


# ── Topics ──────────────────────────────────────────────────

@router.get("/topics")
async def list_topics(request: Request):
    admin = _get_admin(request)
    try:
        return admin.list_topics()
    except Exception as e:
        logger.error(f"Failed to list topics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/topics")
async def create_topic(request: Request):
    admin = _get_admin(request)
    body = await request.json()
    name = body.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="Topic name is required")
    result = admin.create_topic(
        name=name,
        partitions=body.get("partitions", 1),
        replication_factor=body.get("replicationFactor", 1),
        configs=body.get("configs"),
    )
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


# Register more-specific paths BEFORE the catch-all {topic} parameter
@router.get("/topics/{topic}/messages")
async def sample_messages(topic: str, request: Request):
    from config import config as app_config
    if not app_config.SAMPLING_ENABLED:
        raise HTTPException(status_code=403, detail="Message sampling is disabled")
    sampler = _get_sampler(request)
    partition = request.query_params.get("partition")
    offset = request.query_params.get("offset")
    timestamp = request.query_params.get("timestamp")
    limit = min(int(request.query_params.get("limit", "50")), 200)
    if partition is not None and offset is not None:
        messages = sampler.sample_at(topic, int(partition), int(offset), limit)
    elif partition is not None and timestamp is not None:
        messages = sampler.sample_at_timestamp(topic, int(partition), int(timestamp), limit)
    else:
        messages = sampler.sample(topic)
    return {"topic": topic, "messages": messages, "count": len(messages)}


@router.get("/topics/{topic}/consumer-groups")
async def get_topic_consumer_groups(topic: str, request: Request):
    admin = _get_admin(request)
    try:
        return admin.get_topic_consumer_groups(topic)
    except Exception as e:
        logger.error(f"Failed to get consumer groups for topic {topic}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/topics/{topic}/config-diff")
async def get_topic_config_diff(topic: str, request: Request):
    admin = _get_admin(request)
    try:
        return {"topic": topic, "configs": admin.get_topic_config_diff(topic)}
    except Exception as e:
        logger.error(f"Failed to get topic config diff for {topic}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/topics/{topic}/reassign")
async def reassign_partitions(topic: str, request: Request):
    """Start partition reassignment for a topic."""
    admin = _get_admin(request)
    body = await request.json()
    assignments = body.get("assignments")
    if not assignments or not isinstance(assignments, list):
        raise HTTPException(
            status_code=400,
            detail="assignments must be a non-empty list of {partition: int, replicas: [int, ...]}",
        )
    for a in assignments:
        if "partition" not in a or "replicas" not in a:
            raise HTTPException(
                status_code=400,
                detail="Each assignment must have 'partition' (int) and 'replicas' (list of broker IDs)",
            )
    result = admin.reassign_partitions(topic, assignments)
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Reassignment failed"))
    return result


@router.get("/topics/{topic}/reassign")
async def get_reassignment_status(topic: str, request: Request):
    """Check the status of ongoing partition reassignments for a topic."""
    admin = _get_admin(request)
    try:
        return admin.get_partition_reassignment_status(topic)
    except Exception as e:
        logger.error(f"Failed to get reassignment status for {topic}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/topics/{topic}/key-distribution")
async def get_key_distribution(topic: str, request: Request):
    """Sample messages from a topic and return key distribution statistics."""
    admin = _get_admin(request)
    sample_size = min(int(request.query_params.get("sample_size", "1000")), 10000)
    if sample_size < 1:
        raise HTTPException(status_code=400, detail="sample_size must be a positive integer")
    try:
        return admin.get_key_distribution(topic, sample_size=sample_size)
    except Exception as e:
        logger.error(f"Failed to get key distribution for {topic}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/topics/{topic}/search")
async def search_messages(topic: str, request: Request):
    """Search/filter messages in a topic by key pattern, value pattern, partition, and time range."""
    admin = _get_admin(request)
    body = await request.json()
    try:
        results = admin.search_messages(
            topic=topic,
            key_pattern=body.get("key_pattern"),
            value_pattern=body.get("value_pattern"),
            partition=body.get("partition"),
            start_time=body.get("start_time"),
            end_time=body.get("end_time"),
            max_results=body.get("max_results", 100),
        )
        return {"topic": topic, "messages": results, "count": len(results)}
    except Exception as e:
        logger.error(f"Failed to search messages in {topic}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/topics/{topic}")
async def get_topic_detail(topic: str, request: Request):
    admin = _get_admin(request)
    try:
        return admin.get_topic_detail(topic)
    except Exception as e:
        logger.error(f"Failed to get topic detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/topics/{topic}")
async def delete_topic(topic: str, request: Request):
    admin = _get_admin(request)
    result = admin.delete_topic(topic)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.put("/topics/{topic}/config")
async def update_topic_config(topic: str, request: Request):
    admin = _get_admin(request)
    body = await request.json()
    configs = body.get("configs")
    if not configs or not isinstance(configs, dict):
        raise HTTPException(status_code=400, detail="configs must be a non-empty object")
    result = admin.update_topic_config(topic, configs)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/topics/{topic}/partitions")
async def add_topic_partitions(topic: str, request: Request):
    admin = _get_admin(request)
    body = await request.json()
    total = body.get("totalPartitions")
    if not total or not isinstance(total, int) or total < 1:
        raise HTTPException(status_code=400, detail="totalPartitions must be a positive integer")
    result = admin.add_topic_partitions(topic, total)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/topics/{topic}/produce")
async def produce_message(topic: str, request: Request):
    admin = _get_admin(request)
    body = await request.json()
    value = body.get("value", "")
    result = admin.produce_message(
        topic=topic,
        value=value,
        key=body.get("key"),
        headers=body.get("headers"),
        partition=body.get("partition"),
    )
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@router.post("/topics/{topic}/replay")
async def replay_messages(topic: str, request: Request):
    """Copy messages from source topic to target topic."""
    from config import config as app_config
    if not app_config.SAMPLING_ENABLED:
        raise HTTPException(status_code=403, detail="Message sampling is disabled")
    admin = _get_admin(request)
    sampler = _get_sampler(request)
    body = await request.json()
    target_topic = body.get("targetTopic")
    if not target_topic:
        raise HTTPException(status_code=400, detail="targetTopic is required")
    partition = body.get("partition")
    offset = body.get("offset", 0)
    limit = min(body.get("limit", 50), 200)
    if partition is not None:
        messages = sampler.sample_at(topic, int(partition), int(offset), limit)
    else:
        messages = sampler.sample(topic)
    copied = 0
    errors = 0
    for msg in messages:
        value = msg.get("value")
        if isinstance(value, dict):
            import json
            value = json.dumps(value)
        result = admin.produce_message(
            topic=target_topic,
            value=str(value) if value is not None else "",
            key=msg.get("key"),
            headers=None,
        )
        if result.get("success"):
            copied += 1
        else:
            errors += 1
    return {"success": True, "copied": copied, "errors": errors, "total": len(messages)}


# ── Consumer Groups ─────────────────────────────────────────

@router.get("/consumer-groups")
async def list_consumer_groups(request: Request):
    admin = _get_admin(request)
    try:
        return admin.list_consumer_groups()
    except Exception as e:
        logger.error(f"Failed to list consumer groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/consumer-groups/{group}")
async def get_consumer_group_detail(group: str, request: Request):
    admin = _get_admin(request)
    try:
        return admin.get_consumer_group_detail(group)
    except Exception as e:
        logger.error(f"Failed to get consumer group detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/consumer-groups/{group}")
async def delete_consumer_group(group: str, request: Request):
    admin = _get_admin(request)
    result = admin.delete_consumer_group(group)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/consumer-groups/{group}/reset-offsets")
async def reset_offsets(group: str, request: Request):
    admin = _get_admin(request)
    body = await request.json()
    result = admin.reset_offsets(
        group_id=group,
        strategy=body.get("strategy", "latest"),
        topic=body.get("topic"),
        timestamp=body.get("timestamp"),
        offset=body.get("offset"),
    )
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


# ── Brokers ─────────────────────────────────────────────────

@router.get("/brokers")
async def list_brokers(request: Request):
    admin = _get_admin(request)
    try:
        return admin.list_brokers()
    except Exception as e:
        logger.error(f"Failed to list brokers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/brokers/{broker_id}/config")
async def get_broker_config(broker_id: int, request: Request):
    admin = _get_admin(request)
    try:
        configs = admin.describe_broker_config(broker_id)
        return {"brokerId": broker_id, "configs": configs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/brokers/{broker_id}/config")
async def update_broker_config(broker_id: int, request: Request):
    admin = _get_admin(request)
    body = await request.json()
    configs = body.get("configs")
    if not configs or not isinstance(configs, dict):
        raise HTTPException(status_code=400, detail="configs must be a non-empty object")
    result = admin.alter_broker_config(broker_id, configs)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Failed"))
    return result


@router.get("/cluster")
async def get_cluster_info(request: Request):
    admin = _get_admin(request)
    try:
        return admin.get_cluster_info()
    except Exception as e:
        logger.error(f"Failed to get cluster info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cluster/health")
async def get_cluster_health(request: Request):
    admin = _get_admin(request)
    try:
        return admin.get_cluster_health()
    except Exception as e:
        logger.error(f"Failed to get cluster health: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cluster/log-dirs")
async def get_log_dirs(request: Request):
    admin = _get_admin(request)
    try:
        return admin.get_log_dirs()
    except Exception as e:
        logger.error(f"Failed to get log dirs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cluster/elect-leaders")
async def elect_preferred_leaders(request: Request):
    admin = _get_admin(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    topic = body.get("topic")
    try:
        return admin.elect_preferred_leaders(topic)
    except Exception as e:
        logger.error(f"Failed to elect preferred leaders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Schema Registry ────────────────────────────────────────

def _get_schema_registry(request: Request):
    sr = getattr(request.app.state, "schema_registry", None)
    if not sr:
        raise HTTPException(status_code=503, detail="Schema Registry not configured")
    return sr


@router.get("/schema-registry/subjects")
async def list_subjects(request: Request):
    sr = _get_schema_registry(request)
    subjects = sr.list_subjects()
    return {"subjects": subjects}


@router.get("/schema-registry/subjects/{subject}/versions")
async def get_subject_versions(subject: str, request: Request):
    sr = _get_schema_registry(request)
    versions = sr.get_versions(subject)
    return {"subject": subject, "versions": versions}


@router.get("/schema-registry/subjects/{subject}/versions/{version}")
async def get_schema_version(subject: str, version: str, request: Request):
    sr = _get_schema_registry(request)
    ver = "latest" if version == "latest" else int(version)
    schema = sr.get_schema(subject, ver)
    return schema


@router.get("/schema-registry/config")
async def get_global_compatibility(request: Request):
    sr = _get_schema_registry(request)
    level = sr.get_compatibility()
    return {"compatibilityLevel": level}


@router.get("/schema-registry/config/{subject}")
async def get_subject_compatibility(subject: str, request: Request):
    sr = _get_schema_registry(request)
    level = sr.get_compatibility(subject)
    return {"subject": subject, "compatibilityLevel": level}


@router.post("/schema-registry/subjects/{subject}/versions")
async def register_schema(subject: str, request: Request):
    sr = _get_schema_registry(request)
    body = await request.json()
    schema_str = body.get("schema")
    if not schema_str:
        raise HTTPException(status_code=400, detail="schema is required")
    schema_type = body.get("schemaType", "AVRO")
    result = sr.register_schema(subject, schema_str, schema_type)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.delete("/schema-registry/subjects/{subject}")
async def delete_subject(subject: str, request: Request):
    sr = _get_schema_registry(request)
    versions = sr.delete_subject(subject)
    return {"subject": subject, "deletedVersions": versions}


# ── Kafka Connect ──────────────────────────────────────────

def _get_connect(request: Request):
    cc = getattr(request.app.state, "connect_client", None)
    if not cc:
        raise HTTPException(status_code=503, detail="Kafka Connect not configured")
    return cc


@router.get("/connect/connectors")
async def list_connectors(request: Request):
    cc = _get_connect(request)
    names = cc.list_connectors()
    connectors = []
    for name in names:
        status = cc.get_connector_status(name)
        connectors.append({
            "name": name,
            "state": status.get("connector", {}).get("state", "UNKNOWN"),
            "type": status.get("type", "unknown"),
            "tasks": status.get("tasks", []),
        })
    return {"connectors": connectors}


@router.get("/connect/connectors/{name}")
async def get_connector(name: str, request: Request):
    cc = _get_connect(request)
    info = cc.get_connector(name)
    status = cc.get_connector_status(name)
    return {**info, "status": status}


@router.post("/connect/connectors")
async def create_connector(request: Request):
    cc = _get_connect(request)
    body = await request.json()
    name = body.get("name")
    config = body.get("config")
    if not name or not config:
        raise HTTPException(status_code=400, detail="name and config are required")
    result = cc.create_connector(name, config)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.put("/connect/connectors/{name}/config")
async def update_connector(name: str, request: Request):
    cc = _get_connect(request)
    body = await request.json()
    result = cc.update_connector(name, body)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.delete("/connect/connectors/{name}")
async def delete_connector(name: str, request: Request):
    cc = _get_connect(request)
    if cc.delete_connector(name):
        return {"success": True}
    raise HTTPException(status_code=500, detail="Failed to delete connector")


@router.put("/connect/connectors/{name}/pause")
async def pause_connector(name: str, request: Request):
    cc = _get_connect(request)
    if cc.pause_connector(name):
        return {"success": True}
    raise HTTPException(status_code=500, detail="Failed to pause connector")


@router.put("/connect/connectors/{name}/resume")
async def resume_connector(name: str, request: Request):
    cc = _get_connect(request)
    if cc.resume_connector(name):
        return {"success": True}
    raise HTTPException(status_code=500, detail="Failed to resume connector")


@router.post("/connect/connectors/{name}/restart")
async def restart_connector(name: str, request: Request):
    cc = _get_connect(request)
    if cc.restart_connector(name):
        return {"success": True}
    raise HTTPException(status_code=500, detail="Failed to restart connector")


@router.post("/connect/connectors/{name}/tasks/{task_id}/restart")
async def restart_connector_task(name: str, task_id: int, request: Request):
    cc = _get_connect(request)
    if cc.restart_task(name, task_id):
        return {"success": True}
    raise HTTPException(status_code=500, detail=f"Failed to restart task {task_id}")


@router.get("/connect/plugins")
async def list_plugins(request: Request):
    cc = _get_connect(request)
    return {"plugins": cc.get_connector_plugins()}


# ── ACLs ───────────────────────────────────────────────────

@router.get("/acls")
async def list_acls(request: Request):
    admin = _get_admin(request)
    try:
        return admin.list_acls()
    except Exception as e:
        logger.error(f"Failed to list ACLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/acls")
async def create_acl(request: Request):
    admin = _get_admin(request)
    body = await request.json()
    required = ["resourceType", "resourceName", "principal", "operation", "permission"]
    for field in required:
        if not body.get(field):
            raise HTTPException(status_code=400, detail=f"{field} is required")
    try:
        result = admin.create_acl(
            resource_type=body["resourceType"],
            resource_name=body["resourceName"],
            principal=body["principal"],
            operation=body["operation"],
            permission_type=body["permission"],
            pattern_type=body.get("patternType", "LITERAL"),
            host=body.get("host", "*"),
        )
        return result
    except Exception as e:
        logger.error(f"Failed to create ACL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/acls")
async def delete_acl(request: Request):
    admin = _get_admin(request)
    body = await request.json()
    try:
        result = admin.delete_acl(
            resource_type=body.get("resourceType", "ANY"),
            resource_name=body.get("resourceName"),
            principal=body.get("principal"),
            operation=body.get("operation", "ANY"),
            permission_type=body.get("permission", "ANY"),
        )
        return result
    except Exception as e:
        logger.error(f"Failed to delete ACL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Quotas ─────────────────────────────────────────────────

@router.get("/quotas")
async def list_quotas(request: Request):
    admin = _get_admin(request)
    try:
        return admin.list_quotas()
    except Exception as e:
        logger.error(f"Failed to list quotas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quotas")
async def set_quota(request: Request):
    admin = _get_admin(request)
    body = await request.json()
    entity_type = body.get("entityType")
    entity_name = body.get("entityName")
    quotas = body.get("quotas")
    if not entity_type or not entity_name:
        raise HTTPException(status_code=400, detail="entityType and entityName are required")
    if not quotas or not isinstance(quotas, dict):
        raise HTTPException(status_code=400, detail="quotas must be a non-empty object")
    result = admin.set_quota(entity_type=entity_type, entity_name=entity_name, quotas=quotas)
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Failed to set quota"))
    return result


@router.delete("/quotas")
async def delete_quota(request: Request):
    admin = _get_admin(request)
    body = await request.json()
    entity_type = body.get("entityType")
    entity_name = body.get("entityName")
    quota_keys = body.get("quotaKeys")
    if not entity_type or not entity_name:
        raise HTTPException(status_code=400, detail="entityType and entityName are required")
    if not quota_keys or not isinstance(quota_keys, list):
        raise HTTPException(status_code=400, detail="quotaKeys must be a non-empty list of quota key names")
    result = admin.delete_quota(entity_type=entity_type, entity_name=entity_name, quota_keys=quota_keys)
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Failed to delete quota"))
    return result
