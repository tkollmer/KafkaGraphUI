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
    messages = sampler.sample(topic)
    return {"topic": topic, "messages": messages, "count": len(messages)}


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


@router.post("/consumer-groups/{group}/reset-offsets")
async def reset_offsets(group: str, request: Request):
    admin = _get_admin(request)
    body = await request.json()
    result = admin.reset_offsets(
        group_id=group,
        strategy=body.get("strategy", "latest"),
        topic=body.get("topic"),
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


@router.get("/cluster")
async def get_cluster_info(request: Request):
    admin = _get_admin(request)
    try:
        return admin.get_cluster_info()
    except Exception as e:
        logger.error(f"Failed to get cluster info: {e}")
        raise HTTPException(status_code=500, detail=str(e))
