from datetime import datetime
from typing import Awaitable, Callable, Literal, Optional, TypeVar, Generic, Protocol, Dict, Any

from pydantic import BaseModel, Field

ResultT = TypeVar("ResultT")

# models


class FPJobStatus(BaseModel, Generic[ResultT]):
    id: str
    status: Literal["new", "running", "completed", "failed"] = Field(
        default="running",
        description="""The current status of the job.
        Possible values are:
        - 'running': The job is currently being processed.
        - 'completed': The job has finished successfully.
        - 'failed': The job has encountered an error and did not complete successfully.""",
        example="running",
    )
    created_at: datetime = Field(
        default_factory=datetime.now,
        description="The time when the job was created.",
    )
    started_at: Optional[datetime] = Field(
        default=None,
        description="The time when the job started running. This is set when the job is picked up by a worker.",
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        description="The time when the job completed. This is set when the job is finished, either successfully or with an error.",
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        description="The time when the job was last updated. This is set whenever the job's status or progress is updated.",
    )
    locked_until: Optional[datetime] = Field(
        default=None,
        description="""used to prevent multiple workers from processing the same job.
        Also helps to detect stale jobs.
        e.g. if now > locked_until and status is not completed, then the job is stale and can be retried.""",
    )
    progress: Optional[int] = Field(
        default=None,
        description="""Progress of the job, represented as a percentage (0-100).
        Its the responsibility of the function running inside the job to report progress.""",
        ge=0,
        le=100,
        example=50,
    )
    progress_info: Optional[str] = Field(
        default=None,
        description="""Additional information about the job's progress.
        Its the responsibility of the function running inside the job to report progress.""",
        example="Processing step 2 of 5",
    )
    result: Optional[ResultT] = Field(
        default=None,
        description="""The result of the job, if it has completed successfully.
        retrieved automatically by the service after the job is completed.""",
    )
    error: Optional[str] = Field(
        default=None,
        description="""Error message if the job has failed.
        retrieved automatically by the service after the job is completed.""",
    )
    input_args: Optional[list] = Field(
        default=None,
        description="Serialized positional arguments passed to the job function.",
    )
    input_kwargs: Optional[dict] = Field(
        default=None,
        description="Serialized keyword arguments passed to the job function.",
    )


ReportProgressFuncType = Callable[[int, Optional[str]], Awaitable[None]]
