"""
MQTT客户端模块
"""

import json
import time
from typing import Optional, Callable, Dict, Any
from loguru import logger
import paho.mqtt.client as mqtt
from .config import settings
from .config_loader import config_loader

class MQTTClient:
    """MQTT客户端管理器"""
    
    def __init__(self):
        self.client: Optional[mqtt.Client] = None
        self.is_connected = False
        self.message_handlers: Dict[str, Callable] = {}
        self.mqtt_config = {}
        self._setup_client()
    
    def _setup_client(self):
        """设置MQTT客户端"""
        try:
            # 获取MQTT连接配置
            self.mqtt_config = config_loader.get_mqtt_connection_config()
            broker_config = self.mqtt_config.get('broker', {})
            
            if not broker_config:
                logger.error("未找到MQTT连接配置")
                return
            
            self.client = mqtt.Client(
                client_id=broker_config.get('client_id', 'netsrv_client'),
                clean_session=broker_config.get('clean_session', True),
                protocol=mqtt.MQTTv311
            )
            
            # 设置回调函数
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message
            self.client.on_publish = self._on_publish
            
            # 设置认证
            username = broker_config.get('username', '')
            password = broker_config.get('password', '')
            if username and password:
                self.client.username_pw_set(username, password)
            
            # 设置SSL
            ssl_config = broker_config.get('ssl', {})
            if ssl_config.get('enabled', False):
                ca_cert = ssl_config.get('ca_cert')
                client_cert = ssl_config.get('client_cert')
                client_key = ssl_config.get('client_key')
                
                if ca_cert and client_cert and client_key:
                    # 使用证书文件进行双向认证（AWS IoT推荐）
                    self.client.tls_set(
                        ca_certs=ca_cert,
                        certfile=client_cert,
                        keyfile=client_key,
                        cert_reqs=mqtt.ssl.CERT_REQUIRED,
                        tls_version=mqtt.ssl.PROTOCOL_TLSv1_2,
                        ciphers=None
                    )
                    logger.info(f"SSL证书配置完成: CA={ca_cert}, Cert={client_cert}, Key={client_key}")
                else:
                    # 仅启用SSL，不验证证书（不推荐用于生产环境）
                    self.client.tls_set()
                    logger.warning("SSL已启用但未配置证书，连接可能不安全")
            
            logger.info("MQTT客户端设置完成")
            
        except Exception as e:
            logger.error(f"MQTT客户端设置失败: {e}")
    
    def connect(self) -> bool:
        """连接到MQTT代理"""
        try:
            if not self.client:
                self._setup_client()
            
            broker_config = self.mqtt_config.get('broker', {})
            if not broker_config:
                logger.error("MQTT配置未加载")
                return False
            
            self.client.connect(
                broker_config.get('host', 'localhost'),
                broker_config.get('port', 1883),
                keepalive=broker_config.get('keepalive', 60)
            )
            
            # 启动网络循环
            self.client.loop_start()
            
            # 等待连接
            timeout = 10
            start_time = time.time()
            while not self.is_connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            if self.is_connected:
                logger.info(f"MQTT连接成功: {broker_config.get('host')}:{broker_config.get('port')}")
                return True
            else:
                logger.error("MQTT连接超时")
                return False
                
        except Exception as e:
            logger.error(f"MQTT连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开MQTT连接"""
        try:
            if self.client and self.is_connected:
                self.client.loop_stop()
                self.client.disconnect()
                self.is_connected = False
                logger.info("MQTT连接已断开")
        except Exception as e:
            logger.error(f"MQTT断开连接失败: {e}")
    
    def subscribe(self, topic: str, qos: int = 0):
        """订阅主题"""
        try:
            if self.client and self.is_connected:
                result = self.client.subscribe(topic, qos)
                if result[0] == mqtt.MQTT_ERR_SUCCESS:
                    logger.info(f"订阅主题成功: {topic}")
                else:
                    logger.error(f"订阅主题失败: {topic}")
        except Exception as e:
            logger.error(f"订阅主题异常: {topic}, {e}")
    
    def publish(self, topic: str, payload: Any, qos: int = 0, retain: bool = False) -> bool:
        """发布消息"""
        try:
            if self.client and self.is_connected:
                if isinstance(payload, (dict, list)):
                    payload = json.dumps(payload, ensure_ascii=False)
                
                result = self.client.publish(topic, payload, qos, retain)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    logger.debug(f"发布消息成功: {topic}")
                    return True
                else:
                    logger.error(f"发布消息失败: {topic}, 错误码: {result.rc}")
                    return False
            else:
                logger.warning("MQTT客户端未连接，无法发布消息")
                return False
        except Exception as e:
            logger.error(f"发布消息异常: {topic}, {e}")
            return False
    
    def add_message_handler(self, topic: str, handler: Callable):
        """添加消息处理器"""
        self.message_handlers[topic] = handler
        logger.info(f"添加消息处理器: {topic}")
    
    def _on_connect(self, client, userdata, flags, rc):
        """连接回调"""
        if rc == 0:
            self.is_connected = True
            logger.info("MQTT连接已建立")
        else:
            self.is_connected = False
            logger.error(f"MQTT连接失败，错误码: {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """断开连接回调"""
        self.is_connected = False
        if rc != 0:
            logger.warning(f"MQTT意外断开，错误码: {rc}")
        else:
            logger.info("MQTT连接已断开")
    
    def _on_message(self, client, userdata, msg):
        """消息接收回调"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            # 查找对应的处理器
            for pattern, handler in self.message_handlers.items():
                if self._topic_match(pattern, topic):
                    try:
                        handler(topic, payload)
                    except Exception as e:
                        logger.error(f"消息处理器异常: {topic}, {e}")
                    break
            else:
                logger.debug(f"未找到消息处理器: {topic}")
                
        except Exception as e:
            logger.error(f"消息处理异常: {e}")
    
    def _on_publish(self, client, userdata, mid):
        """消息发布回调"""
        logger.debug(f"消息发布完成，消息ID: {mid}")
    
    def _topic_match(self, pattern: str, topic: str) -> bool:
        """主题匹配检查"""
        if pattern == topic:
            return True
        
        # 支持通配符匹配
        if '+' in pattern or '#' in pattern:
            pattern_parts = pattern.split('/')
            topic_parts = topic.split('/')
            
            if len(pattern_parts) > len(topic_parts) and '#' not in pattern_parts:
                return False
            
            for i, pattern_part in enumerate(pattern_parts):
                if i >= len(topic_parts):
                    return False
                
                if pattern_part == '#':
                    return True
                elif pattern_part == '+':
                    continue
                elif pattern_part != topic_parts[i]:
                    return False
            
            return True
        
        return False

# 全局MQTT客户端实例
mqtt_client = MQTTClient()
