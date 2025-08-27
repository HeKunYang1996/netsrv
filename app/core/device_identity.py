"""
设备标识管理器
用于读取和管理设备序列号、产品序列号等标识信息
"""

import os
from pathlib import Path
from typing import Optional
from loguru import logger
from .config_loader import config_loader

class DeviceIdentityManager:
    """设备标识管理器"""
    
    def __init__(self):
        self.product_sn: str = ""
        self.device_sn: str = ""
        self.device_type: str = ""
        self.is_gateway: bool = False
        self._load_identity()
    
    def _load_identity(self):
        """加载设备标识信息"""
        try:
            # 从配置文件加载产品序列号
            self.product_sn = config_loader.get_config('device_identity.product_sn', 'voltageems_netsrv')
            self.device_type = config_loader.get_config('device_identity.device_type', 'gateway')
            self.is_gateway = config_loader.get_config('device_identity.is_gateway', True)
            
            # 获取设备序列号
            config_device_sn = config_loader.get_config('device_identity.device_sn', 'auto')
            
            if config_device_sn == 'auto':
                self.device_sn = self._read_device_serial_number()
            else:
                self.device_sn = config_device_sn
            
            logger.info(f"设备标识加载完成:")
            logger.info(f"  产品序列号: {self.product_sn}")
            logger.info(f"  设备序列号: {self.device_sn}")
            logger.info(f"  设备类型: {self.device_type}")
            logger.info(f"  是否网关: {self.is_gateway}")
            
        except Exception as e:
            logger.error(f"加载设备标识失败: {e}")
            # 使用默认值
            self.product_sn = "voltageems_netsrv"
            self.device_sn = "dev_001"
            self.device_type = "gateway"
            self.is_gateway = True
    
    def _read_device_serial_number(self) -> str:
        """读取设备序列号"""
        try:
            # 尝试从 /proc/device-tree/serial-number 读取
            serial_path = Path("/proc/device-tree/serial-number")
            if serial_path.exists():
                with open(serial_path, 'r') as f:
                    serial = f.read().strip()
                    if serial:
                        logger.info(f"从设备树读取到序列号: {serial}")
                        return serial
            
            # 尝试从 /sys/class/dmi/id/product_serial 读取
            dmi_path = Path("/sys/class/dmi/id/product_serial")
            if dmi_path.exists():
                with open(dmi_path, 'r') as f:
                    serial = f.read().strip()
                    if serial:
                        logger.info(f"从DMI读取到序列号: {serial}")
                        return serial
            
            # 尝试从环境变量读取
            env_serial = os.environ.get('DEVICE_SERIAL_NUMBER')
            if env_serial:
                logger.info(f"从环境变量读取到序列号: {env_serial}")
                return env_serial
            
            # 开发环境使用固定值
            logger.warning("无法读取设备序列号，使用开发环境固定值")
            return "dev_001"
            
        except Exception as e:
            logger.error(f"读取设备序列号失败: {e}")
            return "dev_001"
    
    def get_product_sn(self) -> str:
        """获取产品序列号"""
        return self.product_sn
    
    def get_device_sn(self) -> str:
        """获取设备序列号"""
        return self.device_sn
    
    def get_device_type(self) -> str:
        """获取设备类型"""
        return self.device_type
    
    def is_gateway_device(self) -> bool:
        """是否为网关设备"""
        return self.is_gateway
    
    def format_topic(self, topic_template: str) -> str:
        """格式化MQTT主题，替换占位符"""
        try:
            formatted_topic = topic_template.replace('{productSN}', self.product_sn)
            formatted_topic = formatted_topic.replace('{deviceSN}', self.device_sn)
            return formatted_topic
        except Exception as e:
            logger.error(f"格式化主题失败: {e}")
            return topic_template
    
    def get_formatted_topics(self) -> dict:
        """获取所有格式化的MQTT主题"""
        try:
            topics = config_loader.get_config('mqtt_topics', {})
            formatted_topics = {}
            
            for topic_name, topic_template in topics.items():
                formatted_topics[topic_name] = self.format_topic(topic_template)
            
            return formatted_topics
            
        except Exception as e:
            logger.error(f"获取格式化主题失败: {e}")
            return {}
    
    def get_device_info(self) -> dict:
        """获取设备信息"""
        return {
            'product_sn': self.product_sn,
            'device_sn': self.device_sn,
            'device_type': self.device_type,
            'is_gateway': self.is_gateway,
            'topics': self.get_formatted_topics()
        }
    
    def reload_identity(self):
        """重新加载设备标识"""
        logger.info("重新加载设备标识...")
        self._load_identity()

# 全局设备标识管理器实例
device_identity = DeviceIdentityManager()
