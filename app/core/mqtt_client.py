"""
MQTT客户端模块
"""

import json
import time
import asyncio
import threading
from typing import Optional, Callable, Dict, Any, List
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
        self.connection_callbacks: List[Callable] = []
        self.disconnection_callbacks: List[Callable] = []
        self.reconnect_enabled = True
        self.reconnect_delay = 5
        self.max_reconnect_attempts = 10
        self.current_reconnect_attempts = 0
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
            
            # 应用重连配置
            if 'reconnect_enabled' in self.mqtt_config:
                self.reconnect_enabled = self.mqtt_config['reconnect_enabled']
            if 'reconnect_delay' in self.mqtt_config:
                self.reconnect_delay = self.mqtt_config['reconnect_delay']
            if 'max_reconnect_attempts' in self.mqtt_config:
                self.max_reconnect_attempts = self.mqtt_config['max_reconnect_attempts']
            
            self.client = mqtt.Client(
                client_id=broker_config.get('client_id', 'netsrv_client'),
                clean_session=broker_config.get('clean_session', True),
                protocol=mqtt.MQTTv311
            )
            
            # 设置遗嘱消息
            self._setup_will_message()
            
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
    
    def _setup_will_message(self):
        """设置MQTT遗嘱消息"""
        try:
            # 延迟导入以避免循环导入
            from .device_identity import device_identity
            
            # 获取状态主题模板
            status_topic_template = config_loader.get_config('mqtt_topics.status', 'status/{productSN}/{deviceSN}')
            
            # 格式化状态主题
            will_topic = device_identity.format_topic(status_topic_template)
            
                        # 构建遗嘱消息
            will_message = {
                "type": "offline", 
                "gateway": "",
                "reason": "unexpected_disconnect",
                "timestamp": int(time.time())
            }
            
            # 设置遗嘱消息
            self.client.will_set(
                topic=will_topic,
                payload=json.dumps(will_message, ensure_ascii=False),
                qos=1,
                retain=True
            )
            
            logger.info(f"MQTT遗嘱消息设置完成: {will_topic}")
            
        except Exception as e:
            logger.error(f"设置MQTT遗嘱消息失败: {e}")
    
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
                # 在断开前，确保所有待发送的消息都发送完成
                logger.info("正在发送待发送的消息...")
                
                # 强制处理所有待发送的消息
                for i in range(50):  # 增加处理次数
                    rc = self.client.loop_write()
                    if rc != mqtt.MQTT_ERR_SUCCESS:
                        break
                    time.sleep(0.02)
                
                # 等待消息队列清空
                time.sleep(1.0)
                
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
                    payload_str = json.dumps(payload, ensure_ascii=False)
                else:
                    payload_str = str(payload)
                
                result = self.client.publish(topic, payload_str, qos, retain)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    logger.debug(f"发布消息成功: {topic}")
                    
                    # 立即强制发送消息（不等待确认）
                    try:
                        # 强制处理网络写入操作
                        for i in range(5):  
                            rc = self.client.loop_write()
                            if rc != mqtt.MQTT_ERR_SUCCESS:
                                break
                            time.sleep(0.01)  # 给网络处理一点时间
                        
                        logger.debug(f"消息强制发送: {topic}")
                    except Exception as e:
                        logger.warning(f"强制发送消息失败: {topic}, {e}")
                    
                    # 对于状态消息，输出详细信息
                    if 'status/' in topic:
                        logger.info(f"状态消息发布: {topic} -> {payload_str}")
                    
                    return True
                else:
                    logger.error(f"发布消息失败: {topic}, 错误码: {result.rc}")
                    return False
            else:
                logger.warning(f"MQTT客户端未连接，无法发布消息: {topic}")
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
            self.current_reconnect_attempts = 0  # 重置重连计数
            
            if flags.get('session_present', False):
                logger.info("MQTT连接已建立（恢复会话）")
            else:
                logger.info("MQTT连接已建立（新会话）")
            
            # 立即发送在线消息
            self._send_online_message_sync()
            
            # 调用连接回调
            for callback in self.connection_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        try:
                            loop = asyncio.get_running_loop()
                            loop.create_task(callback())
                        except RuntimeError:
                            # 如果没有事件循环，跳过异步回调
                            logger.warning("跳过异步连接回调：没有运行的事件循环")
                    else:
                        callback()
                except Exception as e:
                    logger.error(f"连接回调执行失败: {e}")
        else:
            self.is_connected = False
            error_messages = {
                1: "连接被拒绝 - 协议版本不正确",
                2: "连接被拒绝 - 客户端ID无效",
                3: "连接被拒绝 - 服务器不可用",
                4: "连接被拒绝 - 用户名或密码错误",
                5: "连接被拒绝 - 未授权"
            }
            error_msg = error_messages.get(rc, f"未知错误码: {rc}")
            logger.error(f"MQTT连接失败: {error_msg}")
    
    def _on_disconnect(self, client, userdata, rc):
        """断开连接回调"""
        self.is_connected = False
        
        # 调用断开连接回调
        for callback in self.disconnection_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(callback())
                    except RuntimeError:
                        # 如果没有事件循环，跳过异步回调
                        logger.warning("跳过异步断开连接回调：没有运行的事件循环")
                else:
                    callback()
            except Exception as e:
                logger.error(f"断开连接回调执行失败: {e}")
        
        if rc != 0:
            logger.warning(f"MQTT意外断开，错误码: {rc}")
            # 启动自动重连
            if self.reconnect_enabled:
                self._start_reconnect()
        else:
            logger.info("MQTT连接已断开")
    
    async def _send_online_message(self):
        """发送在线消息"""
        try:
            # 延迟导入以避免循环导入
            from .device_identity import device_identity
            
            # 获取状态主题
            status_topic_template = config_loader.get_config('mqtt_topics.status', 'status/{productSN}/{deviceSN}')
            status_topic = device_identity.format_topic(status_topic_template)
            
            # 构建在线消息
            online_message = {
                "type": "online",
                "gateway": "",  # 固定为空字符串
                "timestamp": int(time.time())
            }
            
            # 等待确保连接稳定
            await asyncio.sleep(0.5)
            
            # 发送在线消息
            if self.publish(status_topic, online_message, qos=1, retain=True):
                logger.info(f"设备在线消息发送成功: {status_topic}")
            else:
                logger.error("设备在线消息发送失败")
                
        except Exception as e:
            logger.error(f"发送在线消息失败: {e}")
    
    def _send_online_message_sync(self):
        """同步发送在线消息（连接成功后立即调用）"""
        try:
            # 延迟导入以避免循环导入
            from .device_identity import device_identity
            
            # 获取状态主题
            status_topic_template = config_loader.get_config('mqtt_topics.status', 'status/{productSN}/{deviceSN}')
            status_topic = device_identity.format_topic(status_topic_template)
            
            # 构建在线消息
            online_message = {
                "type": "online",
                "gateway": "",
                "timestamp": int(time.time())
            }
            
            # 立即发送在线消息（使用QoS 1确保传输）
            if self.publish(status_topic, online_message, qos=1, retain=True):
                logger.info(f"设备在线消息发送成功: {status_topic}")
                # 额外强制网络处理，确保立即发送
                try:
                    for i in range(10):
                        self.client.loop_write()
                        time.sleep(0.01)
                except:
                    pass
            else:
                logger.error("设备在线消息发送失败")
                
        except Exception as e:
            logger.error(f"同步发送在线消息失败: {e}")
    
    def _start_reconnect(self):
        """启动重连机制"""
        if self.current_reconnect_attempts >= self.max_reconnect_attempts:
            logger.error(f"达到最大重连次数 {self.max_reconnect_attempts}，停止重连")
            return
        
        def reconnect_worker():
            """重连工作线程"""
            while self.current_reconnect_attempts < self.max_reconnect_attempts and not self.is_connected:
                try:
                    self.current_reconnect_attempts += 1
                    logger.info(f"开始第 {self.current_reconnect_attempts} 次重连尝试...")
                    
                    # 等待重连间隔
                    time.sleep(self.reconnect_delay)
                    
                    if not self.is_connected:
                        success = self.connect()
                        if success:
                            logger.info(f"第 {self.current_reconnect_attempts} 次重连成功")
                            break
                        else:
                            logger.error(f"第 {self.current_reconnect_attempts} 次重连失败")
                    
                except Exception as e:
                    logger.error(f"重连异常: {e}")
                    break
            
            if self.current_reconnect_attempts >= self.max_reconnect_attempts and not self.is_connected:
                logger.error(f"重连失败，已达到最大尝试次数 {self.max_reconnect_attempts}")
        
        # 在后台线程中执行重连
        thread = threading.Thread(target=reconnect_worker, daemon=True)
        thread.start()
    
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
    
    def add_connection_callback(self, callback: Callable):
        """添加连接成功回调"""
        self.connection_callbacks.append(callback)
        logger.info("添加连接成功回调")
    
    def add_disconnection_callback(self, callback: Callable):
        """添加断开连接回调"""
        self.disconnection_callbacks.append(callback)
        logger.info("添加断开连接回调")
    
    def set_reconnect_config(self, enabled: bool = True, delay: int = 5, max_attempts: int = 10):
        """设置重连配置"""
        self.reconnect_enabled = enabled
        self.reconnect_delay = delay
        self.max_reconnect_attempts = max_attempts
        logger.info(f"重连配置: enabled={enabled}, delay={delay}s, max_attempts={max_attempts}")
    
    def reset_reconnect_counter(self):
        """重置重连计数器"""
        self.current_reconnect_attempts = 0
        logger.info("重连计数器已重置")

# 全局MQTT客户端实例
mqtt_client = MQTTClient()
