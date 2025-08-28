"""
配置加载器模块
用于加载YAML格式的业务配置文件
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
from loguru import logger
from .config import settings

class ConfigLoader:
    """配置加载器"""
    
    def __init__(self):
        self.config_data: Dict[str, Any] = {}
        self.config_file = "netsrv.yaml"
        self.load_config()
    
    def load_config(self):
        """加载配置文件"""
        try:
            # 构建配置文件路径
            config_path = Path(settings.CONFIG_DIR) / self.config_file
            
            # 如果配置文件不存在，尝试从当前目录加载
            if not config_path.exists():
                current_dir_config = Path("config") / self.config_file
                if current_dir_config.exists():
                    config_path = current_dir_config
                    logger.info(f"从当前目录加载配置文件: {config_path}")
                else:
                    logger.warning(f"配置文件不存在: {config_path}")
                    return
            
            # 读取YAML文件
            with open(config_path, 'r', encoding='utf-8') as file:
                self.config_data = yaml.safe_load(file)
            
            logger.info(f"配置文件加载成功: {config_path}")
            
        except FileNotFoundError:
            logger.error(f"配置文件未找到: {self.config_file}")
        except yaml.YAMLError as e:
            logger.error(f"YAML解析错误: {e}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
    
    def reload_config(self):
        """重新加载配置文件"""
        logger.info("重新加载配置文件...")
        self.load_config()
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        try:
            keys = key.split('.')
            value = self.config_data
            
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default
            
            return value
        except Exception as e:
            logger.warning(f"获取配置失败: {key}, {e}")
            return default
    
    def get_mqtt_targets(self) -> Dict[str, Any]:
        """获取MQTT转发目标配置"""
        return self.get_config('mqtt_targets', {})
    
    def get_http_targets(self) -> Dict[str, Any]:
        """获取HTTP转发目标配置"""
        return self.get_config('http_targets', {})
    
    def get_aliyun_iot_config(self) -> Dict[str, Any]:
        """获取阿里云IoT配置"""
        return self.get_config('aliyun_iot', {})
    
    def get_forward_strategy(self) -> Dict[str, Any]:
        """获取转发策略配置"""
        return self.get_config('forward_strategy', {})
    
    def get_redis_source_config(self) -> Dict[str, Any]:
        """获取Redis数据源配置"""
        return self.get_config('redis_source', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """获取监控配置"""
        return self.get_config('monitoring', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """获取日志配置"""
        return self.get_config('logging', {})
    
    def is_target_enabled(self, target_type: str, target_name: str) -> bool:
        """检查目标是否启用"""
        targets = self.get_config(f'{target_type}_targets', {})
        target = targets.get(target_name, {})
        return target.get('enabled', False)
    
    def get_enabled_targets(self, target_type: str) -> Dict[str, Any]:
        """获取启用的目标配置"""
        targets = self.get_config(f'{target_type}_targets', {})
        return {name: config for name, config in targets.items() 
                if config.get('enabled', False)}
    
    def validate_config(self) -> bool:
        """验证配置文件"""
        try:
            required_sections = [
                'mqtt_targets', 'http_targets', 'forward_strategy'
            ]
            
            for section in required_sections:
                if not self.get_config(section):
                    logger.warning(f"缺少配置节: {section}")
            
            # 检查至少有一个启用的转发目标
            enabled_mqtt = len(self.get_enabled_targets('mqtt')) > 0
            enabled_http = len(self.get_enabled_targets('http')) > 0
            
            if not enabled_mqtt and not enabled_http:
                logger.warning("没有启用的转发目标")
                return False
            
            logger.info("配置文件验证通过")
            return True
            
        except Exception as e:
            logger.error(f"配置文件验证失败: {e}")
            return False
    
    def get_config_summary(self) -> Dict[str, Any]:
        """获取配置摘要"""
        return {
            'mqtt_targets_count': len(self.get_mqtt_targets()),
            'http_targets_count': len(self.get_http_targets()),
            'enabled_mqtt_targets': len(self.get_enabled_targets('mqtt')),
            'enabled_http_targets': len(self.get_enabled_targets('http')),
            'forward_interval': self.get_config('forward_strategy.frequency.interval', 5),
            'batch_size': self.get_config('forward_strategy.frequency.batch_size', 100),
            'config_file': str(Path(settings.CONFIG_DIR) / self.config_file)
        }

    def get_mqtt_connection_config(self) -> Dict[str, Any]:
        """获取MQTT连接配置"""
        mqtt_config = self.get_config('mqtt_connection', {})
        
        # 处理SSL证书路径，转换为绝对路径
        if mqtt_config.get('broker', {}).get('ssl', {}).get('enabled', False):
            ssl_config = mqtt_config['broker']['ssl']
            
            # 检查是否在容器环境中运行
            if os.path.exists('/app/config') and os.path.isdir('/app/config'):
                # 容器环境：使用绝对路径
                config_dir = Path('/app/config')
                logger.debug("检测到容器环境，使用绝对路径")
            else:
                # 本地开发环境：使用相对路径
                config_dir = Path("config")
                logger.debug("检测到本地开发环境，使用相对路径")
            
            # 转换证书路径
            if ssl_config.get('ca_cert'):
                ssl_config['ca_cert'] = str(config_dir / ssl_config['ca_cert'])
            if ssl_config.get('client_cert'):
                ssl_config['client_cert'] = str(config_dir / ssl_config['client_cert'])
            if ssl_config.get('client_key'):
                ssl_config['client_key'] = str(config_dir / ssl_config['client_key'])
            
            logger.debug(f"SSL证书路径: CA={ssl_config.get('ca_cert')}, Cert={ssl_config.get('client_cert')}, Key={ssl_config.get('client_key')}")
        
        return mqtt_config
    
    def get_mqtt_topics_config(self) -> Dict[str, Any]:
        """获取MQTT主题配置"""
        return self.get_config('mqtt_topics', {})
    
    def get_device_identity_config(self) -> Dict[str, Any]:
        """获取设备身份配置"""
        return self.get_config('device_identity', {})
    
    def get_data_report_config(self) -> Dict[str, Any]:
        """获取数据上报配置"""
        return self.get_config('data_report', {})
    
    def get_device_status_config(self) -> Dict[str, Any]:
        """获取设备状态配置"""
        return self.get_config('device_status', {})

# 全局配置加载器实例
config_loader = ConfigLoader()
