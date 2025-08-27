"""
网络服务主程序
"""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger

from app.core.config import settings
from app.core.logger import setup_logger
from app.core.database import redis_manager
from app.core.mqtt_client import mqtt_client
from app.core.config_loader import config_loader
from app.core.device_identity import device_identity
from app.services.data_forwarder import data_forwarder
from app.api.routes import router

# 全局变量
app = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    logger.info("启动网络服务...")
    
    # 初始化日志
    setup_logger()
    
    # 等待Redis连接
    logger.info("等待Redis连接...")
    retry_count = 0
    while not redis_manager.is_connected() and retry_count < 10:
        await asyncio.sleep(1)
        retry_count += 1
    
    if not redis_manager.is_connected():
        logger.warning("Redis连接失败，服务将继续运行")
    
    # 初始化设备标识
    logger.info("初始化设备标识...")
    device_info = device_identity.get_device_info()
    logger.info(f"设备信息: {device_info}")
    
    # 启动数据转发服务
    try:
        await data_forwarder.start()
        logger.info("数据转发服务启动成功")
    except Exception as e:
        logger.error(f"数据转发服务启动失败: {e}")
    
    logger.info("网络服务启动完成")
    
    yield
    
    # 关闭时执行
    logger.info("关闭网络服务...")
    
    # 停止数据转发服务
    try:
        await data_forwarder.stop()
        logger.info("数据转发服务已停止")
    except Exception as e:
        logger.error(f"停止数据转发服务失败: {e}")
    
    # 关闭MQTT连接
    try:
        mqtt_client.disconnect()
        logger.info("MQTT连接已关闭")
    except Exception as e:
        logger.error(f"关闭MQTT连接失败: {e}")
    
    # 关闭Redis连接
    try:
        redis_manager.close()
        logger.info("Redis连接已关闭")
    except Exception as e:
        logger.error(f"关闭Redis连接失败: {e}")
    
    # 关闭HTTP客户端
    try:
        from app.core.http_client import http_client
        await http_client.close()
        http_client.close_sync()
        logger.info("HTTP客户端已关闭")
    except Exception as e:
        logger.error(f"关闭HTTP客户端失败: {e}")
    
    logger.info("网络服务已关闭")

def create_app() -> FastAPI:
    """创建FastAPI应用"""
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="网络服务 - 云端数据转发服务",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )
    
    # 添加CORS中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # 添加全局异常处理器
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"全局异常: {exc}")
        return JSONResponse(
            status_code=500,
            content={"detail": "内部服务器错误"}
        )
    
    # 注册路由
    app.include_router(router)
    
    # 根路径
    @app.get("/")
    async def root():
        return {
            "service": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "status": "running",
            "docs": "/docs",
            "health": "/api/v1/health"
        }
    
    return app

def signal_handler(signum, frame):
    """信号处理器"""
    logger.info(f"收到信号 {signum}，准备关闭服务...")
    if app:
        # 这里可以添加优雅关闭逻辑
        pass
    sys.exit(0)

async def main():
    """主函数"""
    global app
    
    # 设置信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 创建应用
    app = create_app()
    
    # 启动服务器
    import uvicorn
    config = uvicorn.Config(
        app=app,
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
        reload=settings.DEBUG
    )
    
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("收到键盘中断信号，服务正在关闭...")
    except Exception as e:
        logger.error(f"服务运行异常: {e}")
        sys.exit(1)
