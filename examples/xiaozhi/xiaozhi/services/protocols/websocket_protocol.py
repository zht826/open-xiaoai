import asyncio
import json

import websockets

from xiaozhi.ref import get_xiaozhi
from xiaozhi.services.protocols.protocol import Protocol
from xiaozhi.services.protocols.typing import DeviceState
from xiaozhi.utils.config import ConfigManager


class WebsocketProtocol(Protocol):
    def __init__(self):
        super().__init__()
        # 获取配置管理器实例
        self.config = ConfigManager.instance()
        self.websocket = None
        self.server_sample_rate = 24000
        self.server_frame_duration = 60
        self.server_frame_size = int(
            self.server_sample_rate * (self.server_frame_duration / 1000)
        )
        self.connected = False
        self.hello_received = None  # 初始化时先设为 None
        self._connect_lock = asyncio.Lock()
        self._heartbeat_task = None
        self.WEBSOCKET_URL = self.config.get_config("NETWORK.WEBSOCKET_URL")
        self.WEBSOCKET_ACCESS_TOKEN = self.config.get_config(
            "NETWORK.WEBSOCKET_ACCESS_TOKEN"
        )
        self.CLIENT_ID = self.config.get_client_id()
        self.DEVICE_ID = self.config.get_device_id()

    async def _close_websocket(self):
        websocket = self.websocket
        was_connected = self.connected
        self.websocket = None
        self.connected = False
        try:
            if was_connected and self.on_audio_channel_closed:
                await self.on_audio_channel_closed()
        finally:
            if websocket is not None:
                await websocket.close()

    async def connect(self) -> bool:
        """连接到WebSocket服务器"""
        async with self._connect_lock:
            if self.is_audio_channel_opened():
                return True
            return await self._connect()

    async def _connect(self) -> bool:
        try:
            await self._close_websocket()

            # 在连接时创建 Event，确保在正确的事件循环中
            self.hello_received = asyncio.Event()

            # 配置连接
            headers = {
                "Authorization": f"Bearer {self.WEBSOCKET_ACCESS_TOKEN}",
                "Protocol-Version": "1",
                "Device-Id": self.DEVICE_ID,  # 获取设备MAC地址
                "Client-Id": self.CLIENT_ID,
            }

            # 建立WebSocket连接
            websocket = await websockets.connect(
                uri=self.WEBSOCKET_URL, additional_headers=headers
            )
            self.websocket = websocket

            # 启动消息处理循环
            asyncio.create_task(self._message_handler(websocket))

            # 发送客户端hello消息
            hello_message = {
                "type": "hello",
                "version": 1,
                "transport": "websocket",
                "audio_params": {
                    "format": "opus",
                    "sample_rate": 16000,
                    "channels": 1,
                    "frame_duration": 60,
                },
            }
            await websocket.send(json.dumps(hello_message))

            # 等待服务器hello响应
            await asyncio.wait_for(self.hello_received.wait(), timeout=10.0)
            if self.websocket is not websocket:
                raise ConnectionError("连接在握手期间关闭")
        except Exception as e:
            await self._close_websocket()
            if self.on_network_error:
                self.on_network_error(f"无法连接服务: {e or type(e).__name__}")
            return False

        self.connected = True
        print("✅ 小智上游 WebSocket 已连接")
        if self.on_audio_channel_opened:
            await self.on_audio_channel_opened()
        return True

    async def _message_handler(self, websocket):
        """处理接收到的WebSocket消息"""
        try:
            async for message in websocket:
                if self.websocket is not websocket:
                    break
                if isinstance(message, str):
                    try:
                        data = json.loads(message)
                        msg_type = data.get("type")
                        if msg_type == "hello":
                            self._handle_server_hello(data)
                        else:
                            if self.on_incoming_json:
                                self.on_incoming_json(data)
                    except json.JSONDecodeError:
                        pass
                elif self.on_incoming_audio:
                    self.on_incoming_audio(message)
        except websockets.ConnectionClosed:
            pass
        finally:
            if self.websocket is websocket:
                print("⚠️ 小智上游 WebSocket 已断开")
                await self._close_websocket()

    async def send_audio(self, frames: list[bytes]):
        """发送音频数据"""
        if not self.is_audio_channel_opened():
            return

        websocket = self.websocket
        try:
            for frame in frames:
                await websocket.send(frame)
        except websockets.ConnectionClosed:
            if self.websocket is websocket:
                await self._close_websocket()

    async def send_text(self, message: str):
        """发送文本消息"""
        if not self.is_audio_channel_opened():
            raise ConnectionError("小智上游 WebSocket 未连接")

        websocket = self.websocket
        try:
            await websocket.send(message)
        except websockets.ConnectionClosed as error:
            if self.websocket is websocket:
                await self._close_websocket()
            raise ConnectionError("小智上游 WebSocket 已断开") from error

    def is_audio_channel_opened(self) -> bool:
        """检查音频通道是否打开"""
        return self.websocket is not None and self.connected

    async def open_audio_channel(self) -> bool:
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self.heartbeat())
        return await self.connect()

    def _handle_server_hello(self, data: dict):
        # 保留默认 24 kHz，小智服务端返回的采样率可能与实际音频不同。
        if data.get("transport") == "websocket":
            self.hello_received.set()

    async def close_audio_channel(self):
        """关闭音频通道"""
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None
        async with self._connect_lock:
            await self._close_websocket()

    async def heartbeat(self):
        while True:
            await asyncio.sleep(1)
            if not self.is_audio_channel_opened():
                await self.connect()
            elif get_xiaozhi().device_state == DeviceState.IDLE:
                try:
                    await self.send_text(
                        json.dumps({"session_id": "", "type": "ping"})
                    )
                except ConnectionError:
                    await self.connect()
