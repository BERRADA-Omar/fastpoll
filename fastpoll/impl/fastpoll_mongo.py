from datetime import UTC, datetime, timedelta
import uuid
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.results import InsertOneResult
from bson import ObjectId

from fastpoll.fastpoll_base import FastPollBase
from fastpoll.fastpoll_models import FPJobStatus, ResultT


class FastPollMongo(FastPollBase[ResultT]):
    """MongoDB implementation of FastPollBase."""

    def __init__(
        self,
        collection: AsyncIOMotorCollection,
        heartbeat_interval_seconds: int = 15,
    ):
        super().__init__(heartbeat_interval_seconds=heartbeat_interval_seconds)
        self.collection = collection

    @classmethod
    async def _create(
        cls,
        mongo_uri: str = "mongodb://localhost:27017",
        db_name: str = "jobsdb",
        collection_name: str = "jobs",
        heartbeat_interval_seconds: int = 15,
        **kwargs
    ) -> "FastPollMongo":
        """Create a new FastPollMongo instance."""
        client = AsyncIOMotorClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Create indexes
        await collection.create_index("status")
        await collection.create_index("locked_until")

        return cls(collection, heartbeat_interval_seconds=heartbeat_interval_seconds)

    async def _create_job(self, func, *args, **kwargs) -> FPJobStatus[ResultT]:
        """Create a new job in MongoDB."""
        now = datetime.now(UTC)

        # Serialize input arguments for logging (Mongo-specific)
        try:
            import json
            def safe_serialize(obj):
                try:
                    return json.dumps(obj)
                except Exception:
                    return str(obj)
            logged_args = [safe_serialize(arg) for arg in args]
            logged_kwargs = {k: safe_serialize(v) for k, v in kwargs.items()}
        except Exception:
            logged_args = str(args)
            logged_kwargs = str(kwargs)

        doc = {
            "status": "new",
            "progress": 0,
            "progress_info": None,
            "result": None,
            "error": None,
            "created_at": now,
            "started_at": now,
            "completed_at": None,
            "updated_at": now,
            "locked_until": now + timedelta(seconds=self.HEARTBEAT_INTERVAL_SECONDS * 2),
            # Log the function input
            "input_args": logged_args,
            "input_kwargs": logged_kwargs,
        }

        result: InsertOneResult = await self.collection.insert_one(doc)

        # Update status to running
        await self.collection.update_one({"_id": result.inserted_id}, {"$set": {"status": "running"}})

        job_status = FPJobStatus[ResultT](id=str(result.inserted_id), **doc)
        await self.on_job_created(job_status)
        return job_status

    async def _complete_job(self, job_id: str, result: ResultT) -> None:
        """Mark a job as completed with the given result."""
        now = datetime.now(UTC)
        await self.collection.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": "completed",
                    "progress": 100,
                    "result": result,
                    "completed_at": now,
                    "updated_at": now,
                }
            },
        )
        job_status = await self.get_job_status(job_id)
        await self.on_job_completed(job_status)

    async def _fail_job(self, job_id: str, error: Exception) -> None:
        """Mark a job as failed with the given error."""
        now = datetime.now(UTC)
        await self.collection.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": "failed",
                    "error": str(error),
                    "completed_at": now,
                    "updated_at": now,
                }
            },
        )
        job_status = await self.get_job_status(job_id)
        await self.on_job_failed(job_status)

    async def _report_progress(self, job_id: str, progress: int, progress_info: Optional[str] = None) -> None:
        """Report the progress of a job."""
        now = datetime.now(UTC)
        update_doc = {
            "progress": progress,
            "updated_at": now,
        }

        if progress_info is not None:
            update_doc["progress_info"] = progress_info

        await self.collection.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": update_doc},
        )

    async def _prolong_lock_duration(self, job_id: str) -> None:
        """Extend the lock time for a job to prevent it from being picked up by another worker."""
        now = datetime.now(UTC)
        await self.collection.update_one(
            {"_id": ObjectId(job_id), "status": "running"},
            {"$set": {"locked_until": now + timedelta(seconds=self.HEARTBEAT_INTERVAL_SECONDS * 2)}},
        )

    async def get_job_status(self, job_id: str) -> FPJobStatus[ResultT]:
        """Get the status of a job."""
        doc = await self.collection.find_one({"_id": ObjectId(job_id)})
        if not doc:
            raise ValueError(f"Job {job_id} not found")

        parsed_doc = FPJobStatus[ResultT](
            **{k: v for k, v in doc.items() if k != "_id"},
            id=str(doc["_id"]),
        )

        return parsed_doc
