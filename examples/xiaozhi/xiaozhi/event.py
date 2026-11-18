import asyncio

from config import APP_CONFIG
from xiaozhi.ref import (
    get_audio_codec,
    get_kws,
    get_speaker,
    get_vad,
    get_xiaozhi,
    set_speech_frames,
)
from xiaozhi.services.protocols.typing import AbortReason, DeviceState, ListeningMode
from xiaozhi.utils.base import get_env


class Step:
    idle = "idle"
    on_interrupt = "on_interrupt"
    on_wakeup = "on_wakeup"
    on_tts_start = "on_tts_start"
    on_tts_end = "on_tts_end"
    on_speech = "on_speech"
    on_silence = "on_silence"


class __EventManager:
    def __init__(self):
        self.session_id = 0
        self.current_step = Step.idle
        self.next_step_future = None

    def update_step(self, step: Step, step_data=None):
        if not get_env("CLI"):
            return

        get_xiaozhi().loop.call_soon_threadsafe(self._update_step, step, step_data)

    def _update_step(self, step: Step, step_data):
        if step == Step.on_tts_end and self.current_step in (
            Step.on_interrupt,
            Step.on_tts_end,
        ):
            return

        self.current_step = step
        if self.next_step_future is not None and not self.next_step_future.done():
            self.next_step_future.set_result((step, step_data))

        if step in (Step.idle, Step.on_interrupt, Step.on_wakeup, Step.on_tts_end):
            self.session_id += 1
        if step in (Step.on_interrupt, Step.on_wakeup, Step.on_tts_end):
            asyncio.create_task(self.__start_session(step, self.session_id))

    async def wait_next_step(self, timeout=None):
        current_session = self.session_id
        future = asyncio.get_running_loop().create_future()
        self.next_step_future = future
        try:
            result = await asyncio.wait_for(future, timeout)
        except TimeoutError:
            result = ("timeout", None)
        finally:
            if self.next_step_future is future:
                self.next_step_future = None

        if current_session != self.session_id:
            # 当前 session 已经结束
            return ("interrupted", None)
        return result

    def on_interrupt(self):
        """用户打断（小爱同学）"""
        self.update_step(Step.on_interrupt)

    def on_wakeup(self):
        """用户唤醒（你好小智）"""
        self.update_step(Step.on_wakeup)

    def on_tts_end(self, session_id):
        """TTS结束"""
        self.update_step(Step.on_tts_end)

    def on_tts_start(self, session_id):
        """TTS开始"""
        self.update_step(Step.on_tts_start)

    def on_speech(self, speech_buffer: bytes):
        """检测到声音（开始说话"""
        self.update_step(Step.on_speech, speech_buffer)

    def on_silence(self):
        """检测到静音（说话结束）"""
        self.update_step(Step.on_silence)

    def on_disconnect(self):
        self.update_step(Step.idle)

    async def __start_session(self, trigger, session_id):
        if session_id != self.session_id:
            return

        xiaozhi = get_xiaozhi()
        try:
            await self._run_session(xiaozhi, trigger, session_id)
        except ConnectionError as error:
            if session_id == self.session_id:
                xiaozhi.set_device_state(DeviceState.IDLE)
            print(f"⚠️ 小智会话中断: {error}")

    async def _run_session(self, xiaozhi, trigger, session_id):
        vad = get_vad()
        codec = get_audio_codec()
        speaker = get_speaker()
        protocol = xiaozhi.protocol

        # 先取消之前的 VAD 检测和音频输入输出流
        xiaozhi.set_device_state(DeviceState.IDLE)

        # 小爱同学唤醒时，直接打断
        if trigger == Step.on_interrupt:
            if protocol.is_audio_channel_opened():
                await protocol.send_abort_speaking(AbortReason.ABORT)
            return

        if not await protocol.open_audio_channel() or session_id != self.session_id:
            return
        await protocol.send_abort_speaking(AbortReason.ABORT)
        if session_id != self.session_id:
            return

        # 等待 TTS 余音结束
        if trigger == Step.on_tts_end:
            vad.resume("silence")
            step, _ = await self.wait_next_step()
            if step != Step.on_silence:
                return

        # 检查是否有人说话
        vad.resume("speech")
        step, speech_buffer = await self.wait_next_step(
            timeout=APP_CONFIG["wakeup"]["timeout"]
        )
        if step == "timeout":
            # 如果没人说话，则回到 IDLE 状态
            xiaozhi.set_device_state(DeviceState.IDLE)
            print("👋 已退出唤醒")
            after_wakeup = APP_CONFIG["wakeup"]["after_wakeup"]
            await after_wakeup(speaker)
            return
        if step != Step.on_speech:
            return

        # 开始说话
        set_speech_frames(speech_buffer)
        codec.input_stream.start_stream()  # 开启录音
        await protocol.send_start_listening(ListeningMode.MANUAL)
        if session_id != self.session_id:
            return
        xiaozhi.set_device_state(DeviceState.LISTENING)
        print("🎙️ 小智已开始聆听")

        # 等待说话结束
        vad.resume("silence")
        step, _ = await self.wait_next_step()
        if step != Step.on_silence:
            return

        # 停止说话
        await protocol.send_stop_listening()
        if session_id == self.session_id:
            xiaozhi.set_device_state(DeviceState.IDLE)

    async def wakeup(self, text, source):
        before_wakeup = APP_CONFIG["wakeup"]["before_wakeup"]
        kws = get_kws()
        kws.pause()
        try:
            wakeup = await before_wakeup(get_speaker(), text, source)
        except Exception as error:
            print(f"⚠️ 小智唤醒回调失败: {error}")
            return
        finally:
            kws.resume()
        if wakeup:
            self.on_wakeup()


EventManager = __EventManager()
