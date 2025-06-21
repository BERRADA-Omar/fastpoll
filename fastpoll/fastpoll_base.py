from abc import ABC, abstractmethod
import asyncio
from typing import Optional, Generic, Callable, Awaitable
from fastpoll.fastpoll_models import FPJobStatus, ResultT


# service: FastPollBase


class FastPollBase(ABC, Generic[ResultT]):
    """FastPoll is a high level service for creating long-running, asynchronous functions that can be polled for results.
    Features:
    - Execute any function that is asynchronous
    - Polling mechanism to check task status and retrieve results
    - Stateless by design, allowing for easy scaling and distribution
    - Supports any persistent storage backend that supports atomic operations (mongo, redis, etc.)
    - Provides a simple API for task management
    - Provides a way to track task progress
    - Designed to be easy to use and integrate into existing applications
    """

    def __init__(self, heartbeat_interval_seconds: int = 15):
        """
        Initialize the FastPollBase. Subclasses should call this with their desired heartbeat interval.
        """
        self.HEARTBEAT_INTERVAL_SECONDS = heartbeat_interval_seconds

    @classmethod
    async def create(cls, heartbeat_interval_seconds: int = 15, **kwargs) -> "FastPollBase":
        """
        Factory method to create an instance of FastPollBase. Subclasses should override _create.
        """
        instance = await cls._create(heartbeat_interval_seconds=heartbeat_interval_seconds, **kwargs)
        return instance

    @classmethod
    @abstractmethod
    async def _create(cls, heartbeat_interval_seconds: int = 15, **kwargs) -> "FastPollBase":
        """
        Factory method to create an instance of FastPollBase.
        This method should be implemented by subclasses to provide the actual instantiation logic.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    ### Job Management Methods ###

    async def run_job(self, func, *args, **kwargs) -> str:
        """
        Run a job asynchronously in the background.

        :param func: The function to run.
        :param args: Positional arguments for the function.
        :param kwargs: Keyword arguments for the function.
        :return: A unique identifier for the job.
        """
        stop_event = asyncio.Event()

        init_job_status = await self._create_job(func, *args, **kwargs)
        self.on_job_created(init_job_status)
        heartbeat_task = asyncio.create_task(self.extend_lock(job_id=init_job_status.id, stop_event=stop_event))

        async def report_progress(progress: int, progress_info: Optional[str] = None) -> None:
            await self._report_progress(init_job_status.id, progress, progress_info)

        # Use the correct type for report_progress
        report_progress: Callable[[int, Optional[str]], Awaitable[None]] = report_progress

        async def execute_job():
            try:
                result = await func(*args, **kwargs, report_progress=report_progress)
                await self._complete_job(job_id=init_job_status.id, result=result)
                self.on_job_completed(init_job_status)
            except Exception as e:
                await self._fail_job(job_id=init_job_status.id, error=e)
                self.on_job_failed(init_job_status)
            finally:
                stop_event.set()
                heartbeat_task.cancel()

        # Run the job in the background
        asyncio.create_task(execute_job())
        return init_job_status.id

    # --- Abstract Storage Interface ---
    @abstractmethod
    async def _create_job(self, func, *args, **kwargs) -> FPJobStatus[ResultT]:
        """
        Create a new job to run a function asynchronously. Should store all necessary job metadata.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def _complete_job(self, job_id: str, result: ResultT) -> None:
        """
        Mark a job as completed with the given result.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def _fail_job(self, job_id: str, error: Exception) -> None:
        """
        Mark a job as failed with the given error.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def _report_progress(self, job_id: str, progress: int, progress_info: Optional[str] = None) -> None:
        """
        Report the progress of a job.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def _prolong_lock_duration(self, job_id: str) -> None:
        """
        Prolong the lock duration for a job to prevent it from being picked up by another worker.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def get_job_status(self, job_id: str) -> FPJobStatus[ResultT]:
        """
        Get the status of a job.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    ### Lock Management Methods ###

    async def extend_lock(self, job_id: str, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                await asyncio.sleep(self.HEARTBEAT_INTERVAL_SECONDS)
                await self._prolong_lock_duration(job_id=job_id)
        except asyncio.CancelledError:
            pass

    # --- Optional Hooks for Extensibility ---
    async def on_job_created(self, job_status: FPJobStatus[ResultT]):
        """
        Optional async hook called after a job is created. Subclasses can override for logging, metrics, etc.
        """
        pass

    async def on_job_completed(self, job_status: FPJobStatus[ResultT]):
        """
        Optional async hook called after a job is completed. Subclasses can override for logging, metrics, etc.
        """
        pass

    async def on_job_failed(self, job_status: FPJobStatus[ResultT]):
        """
        Optional async hook called after a job fails. Subclasses can override for logging, metrics, etc.
        """
        pass
