"""
API路由模块
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Any
from app.services.data_forwarder import data_forwarder
from app.services.alarm_broadcaster import alarm_broadcaster
from app.core.database import redis_manager
from app.core.mqtt_client import mqtt_client
from app.core.config import settings

router = APIRouter(prefix="/netApi", tags=["网络服务"])

class AlarmRequest(BaseModel):
    """告警请求模型"""
    data: Dict[str, Any] = Field(..., description="告警数据，JSON对象格式")
    
    class Config:
        schema_extra = {
            "example": {
                "data": {
                    "alarm_id": "ALM001",
                    "alarm_type": "high_temperature",
                    "level": "critical",
                    "message": "设备温度过高",
                    "device_id": "DEV001",
                    "value": 85.5,
                    "threshold": 80.0,
                    "location": "机房A-机柜01",
                    "description": "传感器检测到设备温度超过阈值"
                }
            }
        }

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
async def broadcast_alarm(alarm_request: AlarmRequest):
    """
    告警广播接口
    
    接收JSON格式的告警数据，并通过MQTT发布到 alarm/{productSN}/{deviceSN} 主题
    """
    try:
        # 验证告警数据格式
        is_valid, error_msg = alarm_broadcaster.validate_alarm_data(alarm_request.data)
        if not is_valid:
            raise HTTPException(status_code=400, detail=f"告警数据格式错误: {error_msg}")
        
        # 检查MQTT连接状态
        if not mqtt_client.is_connected:
            raise HTTPException(status_code=503, detail="MQTT服务未连接，无法发送告警")
        
        # 广播告警消息
        success = alarm_broadcaster.broadcast_alarm(alarm_request.data)
        
        if success:
            return {
                "status": "success",
                "message": "告警消息发送成功",
                "topic": alarm_broadcaster.get_alarm_topic(),
                "timestamp": alarm_request.data.get("timestamp"),
                "data_size": len(str(alarm_request.data))
            }
        else:
            raise HTTPException(status_code=500, detail="告警消息发送失败")
            
    except HTTPException:
        raise
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