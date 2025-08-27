"""
API路由模块
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import Dict, List, Any
from loguru import logger
from app.services.data_forwarder import data_forwarder
from app.core.database import redis_manager
from app.core.mqtt_client import mqtt_client
from app.core.config import settings

router = APIRouter(prefix="/api/v1", tags=["网络服务"])

@router.get("/health")
async def health_check():
    """健康检查接口"""
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "redis_connected": redis_manager.is_connected(),
        "mqtt_connected": mqtt_client.is_connected,
        "forwarder_running": data_forwarder.is_running
    }

@router.get("/status")
async def get_service_status():
    """获取服务状态"""
    return {
        "service_name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "debug_mode": settings.DEBUG,
        "redis_status": {
            "host": settings.REDIS_HOST,
            "port": settings.REDIS_PORT,
            "connected": redis_manager.is_connected()
        },
        "mqtt_status": {
            "broker": f"{settings.MQTT_BROKER_HOST}:{settings.MQTT_BROKER_PORT}",
            "connected": mqtt_client.is_connected,
            "client_id": settings.MQTT_CLIENT_ID
        },
        "forwarder_status": {
            "running": data_forwarder.is_running,
            "config_count": len(data_forwarder.forward_configs),
            "last_forward_time": data_forwarder.last_forward_time
        }
    }

@router.post("/forwarder/start")
async def start_forwarder():
    """启动数据转发服务"""
    try:
        await data_forwarder.start()
        return {"message": "数据转发服务启动成功", "status": "running"}
    except Exception as e:
        logger.error(f"启动数据转发服务失败: {e}")
        raise HTTPException(status_code=500, detail=f"启动失败: {str(e)}")

@router.post("/forwarder/stop")
async def stop_forwarder():
    """停止数据转发服务"""
    try:
        await data_forwarder.stop()
        return {"message": "数据转发服务停止成功", "status": "stopped"}
    except Exception as e:
        logger.error(f"停止数据转发服务失败: {e}")
        raise HTTPException(status_code=500, detail=f"停止失败: {str(e)}")

@router.get("/forwarder/configs")
async def get_forward_configs():
    """获取转发配置列表"""
    return {
        "configs": data_forwarder.forward_configs,
        "count": len(data_forwarder.forward_configs)
    }

@router.post("/forwarder/configs")
async def add_forward_config(config: Dict[str, Any]):
    """添加转发配置"""
    try:
        # 验证配置
        required_fields = ['name', 'type']
        for field in required_fields:
            if field not in config:
                raise HTTPException(status_code=400, detail=f"缺少必需字段: {field}")
        
        # 添加配置
        data_forwarder.add_forward_config(config)
        
        return {
            "message": "转发配置添加成功",
            "config": config,
            "total_configs": len(data_forwarder.forward_configs)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"添加转发配置失败: {e}")
        raise HTTPException(status_code=500, detail=f"添加失败: {str(e)}")

@router.delete("/forwarder/configs/{config_name}")
async def remove_forward_config(config_name: str):
    """移除转发配置"""
    try:
        data_forwarder.remove_forward_config(config_name)
        return {
            "message": "转发配置移除成功",
            "removed_config": config_name,
            "total_configs": len(data_forwarder.forward_configs)
        }
    except Exception as e:
        logger.error(f"移除转发配置失败: {e}")
        raise HTTPException(status_code=500, detail=f"移除失败: {str(e)}")

@router.get("/redis/keys")
async def get_redis_keys(pattern: str = None, limit: int = 100):
    """获取Redis键列表"""
    try:
        redis_client = redis_manager.get_client()
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis连接不可用")
        
        # 构建搜索模式
        search_pattern = pattern or "comsrv:*"
        
        # 获取键列表
        keys = redis_client.keys(search_pattern)
        keys = keys[:limit] if limit > 0 else keys
        
        # 获取键的详细信息
        key_details = []
        for key in keys:
            try:
                key_type = redis_client.type(key)
                ttl = redis_client.ttl(key)
                key_details.append({
                    "key": key,
                    "type": key_type,
                    "ttl": ttl
                })
            except Exception as e:
                key_details.append({
                    "key": key,
                    "type": "unknown",
                    "ttl": -1,
                    "error": str(e)
                })
        
        return {
            "pattern": search_pattern,
            "keys": key_details,
            "total_count": len(keys)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取Redis键失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")

@router.get("/redis/keys/{key}")
async def get_redis_value(key: str):
    """获取Redis键值"""
    try:
        redis_client = redis_manager.get_client()
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis连接不可用")
        
        value = redis_client.get(key)
        if value is None:
            raise HTTPException(status_code=404, detail="键不存在")
        
        return {
            "key": key,
            "value": value,
            "exists": True
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取Redis值失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")

@router.post("/mqtt/publish")
async def publish_mqtt_message(topic: str, message: Dict[str, Any], qos: int = 0):
    """发布MQTT消息"""
    try:
        if not mqtt_client.is_connected:
            raise HTTPException(status_code=503, detail="MQTT连接不可用")
        
        success = mqtt_client.publish(topic, message, qos)
        if success:
            return {
                "message": "MQTT消息发布成功",
                "topic": topic,
                "qos": qos
            }
        else:
            raise HTTPException(status_code=500, detail="MQTT消息发布失败")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"发布MQTT消息失败: {e}")
        raise HTTPException(status_code=500, detail=f"发布失败: {str(e)}")

@router.post("/mqtt/subscribe")
async def subscribe_mqtt_topic(topic: str, qos: int = 0):
    """订阅MQTT主题"""
    try:
        if not mqtt_client.is_connected:
            raise HTTPException(status_code=503, detail="MQTT连接不可用")
        
        mqtt_client.subscribe(topic, qos)
        return {
            "message": "MQTT主题订阅成功",
            "topic": topic,
            "qos": qos
        }
    except Exception as e:
        logger.error(f"订阅MQTT主题失败: {e}")
        raise HTTPException(status_code=500, detail=f"订阅失败: {str(e)}")

@router.get("/mqtt/connection")
async def get_mqtt_connection_status():
    """获取MQTT连接状态"""
    return {
        "connected": mqtt_client.is_connected,
        "broker": f"{settings.MQTT_BROKER_HOST}:{settings.MQTT_BROKER_PORT}",
        "client_id": settings.MQTT_CLIENT_ID,
        "keepalive": settings.MQTT_KEEPALIVE
    }

@router.post("/mqtt/connect")
async def connect_mqtt():
    """手动连接MQTT"""
    try:
        success = mqtt_client.connect()
        if success:
            return {"message": "MQTT连接成功", "status": "connected"}
        else:
            raise HTTPException(status_code=500, detail="MQTT连接失败")
    except Exception as e:
        logger.error(f"MQTT连接失败: {e}")
        raise HTTPException(status_code=500, detail=f"连接失败: {str(e)}")

@router.post("/mqtt/disconnect")
async def disconnect_mqtt():
    """断开MQTT连接"""
    try:
        mqtt_client.disconnect()
        return {"message": "MQTT连接已断开", "status": "disconnected"}
    except Exception as e:
        logger.error(f"断开MQTT连接失败: {e}")
        raise HTTPException(status_code=500, detail=f"断开失败: {str(e)}")
