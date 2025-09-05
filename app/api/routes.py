"""
API路由模块
"""

from fastapi import APIRouter, HTTPException, Request
from typing import Dict, Any
from app.services.data_forwarder import data_forwarder
from app.services.alarm_broadcaster import alarm_broadcaster
from app.core.database import redis_manager
from app.core.mqtt_client import mqtt_client
from app.core.config import settings

router = APIRouter(prefix="/netApi", tags=["网络服务"])

# 告警接口已改为直接接收JSON数据，不再需要特定的请求模型

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

@router.post("/alarm/broadcast")
async def broadcast_alarm(request: Request):
    """
    告警广播接口
    
    接收任意JSON格式的告警数据，并原样通过MQTT发布到 alarm/{productSN}/{deviceSN} 主题
    """
    try:
        # 直接获取原始JSON数据
        alarm_data = await request.json()
        
        # 验证告警数据格式
        is_valid, error_msg = alarm_broadcaster.validate_alarm_data(alarm_data)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"告警数据格式错误: {error_msg}")
        
        # 检查MQTT连接状态
        if not mqtt_client.is_connected:
            raise HTTPException(status_code=503, detail="MQTT服务未连接，无法发送告警")
        
        # 广播告警消息（原样转发）
        success = alarm_broadcaster.broadcast_alarm(alarm_data)
        
        if success:
            return {
                "status": "success",
                "message": "告警消息发送成功",
                "topic": alarm_broadcaster.get_alarm_topic(),
                "timestamp": alarm_data.get("timestamp"),
                "data_size": len(str(alarm_data))
            }
        else:
            raise HTTPException(status_code=500, detail="告警消息发送失败")
            
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"JSON格式错误: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务内部错误: {str(e)}")

@router.get("/alarm/config")
async def get_alarm_config():
    """
    获取告警配置信息
    """
    try:
        return {
            "status": "success",
            "config": {
                "alarm_topic": alarm_broadcaster.get_alarm_topic(),
                "mqtt_connected": mqtt_client.is_connected,
                "service_status": "running" if alarm_broadcaster.formatted_alarm_topic else "not_configured"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取告警配置失败: {str(e)}")