"""
API路由模块
"""

from fastapi import APIRouter
from app.services.data_forwarder import data_forwarder
from app.core.database import redis_manager
from app.core.mqtt_client import mqtt_client
from app.core.config import settings

router = APIRouter(prefix="/netApi", tags=["网络服务"])

@router.get("/health")
async def health_check():
    """健康检查接口"""
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "redis_connected": redis_manager.is_connected(),
        "mqtt_connected": mqtt_client.is_connected,
        "forwarder_running": data_forwarder.is_running,
        "message": "网络服务运行正常"
    }