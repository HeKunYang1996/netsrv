"""
数据转发服务模块
"""

import json
import asyncio
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict
from loguru import logger
from app.core.database import redis_manager
from app.core.mqtt_client import mqtt_client
from app.core.config_loader import config_loader
from app.core.device_identity import device_identity

class DataForwarder:
    """数据转发服务"""
    
    def __init__(self):
        self.is_running = False
        self.forward_task: Optional[asyncio.Task] = None
        self.last_forward_time = 0
        self.device_online_sent = False
        
    async def start(self):
        """启动数据转发服务"""
        if self.is_running:
            logger.warning("数据转发服务已在运行")
            return
        
        logger.info("启动数据转发服务...")
        self.is_running = True
        
        # 启动MQTT连接
        if not mqtt_client.is_connected:
            if not mqtt_client.connect():
                logger.error("MQTT连接失败，数据转发服务无法启动")
                self.is_running = False
                return
        
        # 发送设备上线消息
        await self._send_device_online()
        
        # 启动转发任务
        self.forward_task = asyncio.create_task(self._forward_loop())
        logger.info("数据转发服务启动成功")
    
    async def stop(self):
        """停止数据转发服务"""
        if not self.is_running:
            return
        
        logger.info("停止数据转发服务...")
        self.is_running = False
        
        # 发送设备下线消息
        await self._send_device_offline()
        
        if self.forward_task:
            self.forward_task.cancel()
            try:
                await self.forward_task
            except asyncio.CancelledError:
                pass
        
        logger.info("数据转发服务已停止")
    
    async def _send_device_online(self):
        """发送设备上线消息"""
        try:
            if self.device_online_sent:
                return
            
            # 获取状态主题
            status_topic = device_identity.format_topic(
                config_loader.get_config('mqtt_topics.status', 'status/{productSN}/{deviceSN}')
            )
            
            # 构建上线消息
            online_message = {
                "type": "online",
                "gateway": "" if device_identity.is_gateway_device() else device_identity.get_device_sn()
            }
            
            # 发送上线消息
            if mqtt_client.publish(status_topic, online_message, qos=1, retain=True):
                logger.info(f"设备上线消息发送成功: {status_topic}")
                self.device_online_sent = True
            else:
                logger.error("设备上线消息发送失败")
                
        except Exception as e:
            logger.error(f"发送设备上线消息失败: {e}")
    
    async def _send_device_offline(self):
        """发送设备下线消息"""
        try:
            # 获取状态主题
            status_topic = device_identity.format_topic(
                config_loader.get_config('mqtt_topics.status', 'status/{productSN}/{deviceSN}')
            )
            
            # 构建下线消息
            offline_message = {
                "type": "offline",
                "gateway": "" if device_identity.is_gateway_device() else device_identity.get_device_sn()
            }
            
            # 发送下线消息
            if mqtt_client.publish(status_topic, offline_message, qos=1, retain=True):
                logger.info(f"设备下线消息发送成功: {status_topic}")
            else:
                logger.error("设备下线消息发送失败")
                
        except Exception as e:
            logger.error(f"发送设备下线消息失败: {e}")
    
    async def _forward_loop(self):
        """数据转发主循环"""
        while self.is_running:
            try:
                current_time = time.time()
                
                # 检查是否需要转发数据
                interval = config_loader.get_config('data_report.interval', 5)
                if current_time - self.last_forward_time >= interval:
                    await self._forward_data()
                    self.last_forward_time = current_time
                
                # 等待下次检查
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"数据转发循环异常: {e}")
                await asyncio.sleep(5)
    
    async def _forward_data(self):
        """执行数据转发"""
        try:
            # 从Redis获取数据
            data = await self._fetch_data_from_redis()
            if not data:
                return
            
            # 按数据类型分组并分别上送
            await self._send_grouped_data(data)
            
        except Exception as e:
            logger.error(f"数据转发异常: {e}")
    
    async def _fetch_data_from_redis(self) -> Optional[List[Dict]]:
        """从Redis获取数据"""
        try:
            redis_client = redis_manager.get_client()
            if not redis_client:
                logger.warning("Redis连接不可用")
                return None
            
            # 获取订阅模式
            patterns = config_loader.get_config('redis_source.subscribe_patterns', [])
            if not patterns:
                logger.warning("未配置Redis订阅模式")
                return None
            
            # 获取所有匹配的键
            all_data = []
            for pattern in patterns:
                try:
                    keys = redis_client.keys(pattern)
                    logger.debug(f"模式 {pattern} 找到 {len(keys)} 个键")
                    
                    for key in keys:
                        try:
                            # 检查键的类型
                            key_type = redis_client.type(key)
                            logger.debug(f"键 {key} 类型: {key_type}")
                            
                            if key_type == 'string':
                                # 字符串类型
                                value = redis_client.get(key)
                                if value:
                                    try:
                                        parsed_value = json.loads(value)
                                        all_data.append({
                                            'key': key,
                                            'value': parsed_value,
                                            'timestamp': datetime.now().isoformat()
                                        })
                                    except json.JSONDecodeError:
                                        all_data.append({
                                            'key': key,
                                            'value': value,
                                            'timestamp': datetime.now().isoformat()
                                        })
                            
                            elif key_type == 'hash':
                                # 哈希表类型
                                hash_data = redis_client.hgetall(key)
                                if hash_data:
                                    all_data.append({
                                        'key': key,
                                        'value': hash_data,
                                        'timestamp': datetime.now().isoformat()
                                    })
                            
                            elif key_type == 'list':
                                # 列表类型
                                list_data = redis_client.lrange(key, 0, -1)
                                if list_data:
                                    all_data.append({
                                        'key': key,
                                        'value': list_data,
                                        'timestamp': datetime.now().isoformat()
                                    })
                            
                            elif key_type == 'set':
                                # 集合类型
                                set_data = list(redis_client.smembers(key))
                                if set_data:
                                    all_data.append({
                                        'key': key,
                                        'value': set_data,
                                        'timestamp': datetime.now().isoformat()
                                    })
                            
                            else:
                                logger.debug(f"跳过不支持的键类型: {key} ({key_type})")
                                
                        except Exception as e:
                            logger.warning(f"处理键 {key} 失败: {e}")
                            
                except Exception as e:
                    logger.warning(f"获取Redis模式数据失败: {pattern}, {e}")
            
            logger.debug(f"总共获取到 {len(all_data)} 条数据")
            
            # 应用过滤规则
            filtered_data = self._apply_filters(all_data)
            logger.debug(f"过滤后剩余 {len(filtered_data)} 条数据")
            
            return filtered_data
            
        except Exception as e:
            logger.error(f"从Redis获取数据失败: {e}")
            return None
    
    def _apply_filters(self, data: List[Dict]) -> List[Dict]:
        """应用数据过滤规则"""
        try:
            filters = config_loader.get_config('redis_source.filters', {})
            if not filters.get('enabled', True):
                return data
            
            filtered_data = data
            
            # 应用排除模式过滤
            exclude_patterns = filters.get('exclude_patterns', [])
            for pattern in exclude_patterns:
                filtered_data = [item for item in filtered_data 
                               if not self._pattern_match(pattern, item['key'])]
            
            return filtered_data
            
        except Exception as e:
            logger.error(f"应用数据过滤失败: {e}")
            return data
    
    def _pattern_match(self, pattern: str, key: str) -> bool:
        """检查键是否匹配模式"""
        try:
            if '*' in pattern:
                pattern_parts = pattern.split('*')
                if len(pattern_parts) == 2:
                    return key.startswith(pattern_parts[0]) and key.endswith(pattern_parts[1])
                elif len(pattern_parts) == 1:
                    return key.startswith(pattern_parts[0])
            return pattern == key
        except:
            return False
    
    def _parse_key_format(self, key: str) -> Dict[str, str]:
        """解析键格式 A:B:C"""
        try:
            parts = key.split(':')
            if len(parts) >= 3:
                return {
                    'service': parts[0],      # A - 服务名 (comsrv, modsrv)
                    'channel': parts[1],      # B - 通道ID
                    'data_type': parts[2]     # C - 数据类型 (T, S, C, A等)
                }
            elif len(parts) == 2:
                return {
                    'service': parts[0],
                    'channel': parts[1],
                    'data_type': 'unknown'
                }
            else:
                return {
                    'service': key,
                    'channel': 'unknown',
                    'data_type': 'unknown'
                }
        except:
            return {
                'service': 'unknown',
                'channel': 'unknown',
                'data_type': 'unknown'
            }
    
    def _convert_hash_values(self, hash_data: Dict[str, str]) -> Dict[str, Any]:
        """转换hash值中的数字字符串为float"""
        converted_data = {}
        try:
            for key, value in hash_data.items():
                # 尝试转换为float
                try:
                    float_value = float(value)
                    # 检查是否为整数
                    if float_value.is_integer():
                        converted_data[key] = int(float_value)
                    else:
                        converted_data[key] = float_value
                except (ValueError, TypeError):
                    # 如果转换失败，保持原值
                    converted_data[key] = value
        except Exception as e:
            logger.warning(f"转换hash值失败: {e}")
            return hash_data
        
        return converted_data
    
    async def _send_grouped_data(self, data: List[Dict]):
        """按数据类型分组并分别上送数据"""
        try:
            # 按数据类型分组
            grouped_data = defaultdict(list)
            
            for item in data:
                key_info = self._parse_key_format(item['key'])
                group_key = f"{key_info['service']}:{key_info['channel']}:{key_info['data_type']}"
                grouped_data[group_key].append(item)
            
            # 获取批量大小配置
            batch_size = config_loader.get_config('data_report.batch_size', 50)
            
            # 处理每个分组
            for group_key, group_items in grouped_data.items():
                # 如果分组内的点位超过批量大小，需要分割
                if len(group_items) > batch_size:
                    # 分割成多个批次
                    for i in range(0, len(group_items), batch_size):
                        batch_items = group_items[i:i + batch_size]
                        await self._send_property_data(batch_items, group_key)
                        logger.info(f"分组 {group_key} 分割发送第 {i//batch_size + 1} 批，包含 {len(batch_items)} 个点位")
                else:
                    # 直接发送整个分组
                    await self._send_property_data(group_items, group_key)
                    logger.info(f"分组 {group_key} 发送完成，包含 {len(group_items)} 个点位")
                    
        except Exception as e:
            logger.error(f"分组发送数据失败: {e}")
    
    async def _send_property_data(self, data: List[Dict], group_key: str):
        """发送点位数据上报"""
        try:
            # 获取属性主题
            property_topic = device_identity.format_topic(
                config_loader.get_config('mqtt_topics.property', 'property/{productSN}/{deviceSN}')
            )
            
            # 构建点位数据
            property_data = []
            
            for item in data:
                key_info = self._parse_key_format(item['key'])
                
                # 构建点位标识 (格式: comsrv_1)
                point_id = f"{key_info['service']}_{key_info['channel']}"
                
                # 处理不同类型的值
                if isinstance(item['value'], dict):
                    # 如果是hash类型，转换数字字符串为float
                    converted_value = self._convert_hash_values(item['value'])
                else:
                    # 其他类型保持原值
                    converted_value = item['value']
                
                point_data = {
                    "point": point_id,
                    "data_type": key_info['data_type'],
                    "value": converted_value
                }
                property_data.append(point_data)
            
            if property_data:
                # 构建消息
                current_timestamp = int(time.time())  # Unix时间戳
                message = {
                    "timestamp": current_timestamp,
                    "property": property_data
                }
                
                # 发送数据
                if mqtt_client.publish(property_topic, message, qos=1):
                    logger.debug(f"点位数据上报成功: {property_topic}, 组: {group_key}, 数据量: {len(property_data)}")
                else:
                    logger.warning(f"点位数据上报失败: {group_key}")
            
        except Exception as e:
            logger.error(f"发送点位数据失败: {e}")

# 全局数据转发器实例
data_forwarder = DataForwarder()
