import mimetypes
import os
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator, Literal
from uuid import uuid4

import boto3
import botocore.exceptions
from pydantic import BaseModel, Field, ValidationError
from yt_dlp import YoutubeDL
from yt_dlp.utils import YoutubeDLError

logger = getLogger()


class AmaterusEnqueueDownloadVideoEvent(BaseModel):
    url: str = Field(validation_alias="Url")
    format: str | None = Field(validation_alias="Format", default=None)
    format_sort: list[str] | None = Field(validation_alias="FormatSort", default=None)


class AmaterusDownloadVideoSuccessResponse(BaseModel):
    result: Literal["success"] = Field(serialization_alias="Result")
    message: str = Field(serialization_alias="Message")
    key: str = Field(serialization_alias="Key")


class AmaterusDownloadVideoErrorResponse(BaseModel):
    result: Literal["error"] = Field(serialization_alias="Result")
    message: str = Field(serialization_alias="Message")


@dataclass
class DownloadVideoResult:
    video_file: Path
    content_type: str


class DownloadVideoError(Exception):
    pass


def match_filter_normal_video_or_live_vod(info: dict) -> str | None:
    """
    Accept:
    - A normal video (live_status: not_live)
    - A live streaming video whose VOD is processed (live_status: was_live)

    Reject:
    - A live streaming video which is streaming currently (live_status: is_live)
    - A live streaming video whose VOD is not processed yet (live_status: post_live)
    - Other videos
    """
    live_status = info.get("live_status")

    if live_status == "not_live":
        # normal video
        return None

    if live_status == "was_live":
        # VOD is already processed
        return None

    if live_status == "is_live":
        return "The video is a live streaming video which is streaming currently"

    if live_status == "post_live":
        return "The video is a live streaming video whose VOD is not processed yet"

    return f"The video has unknown live_status: {live_status}"


@contextmanager
def download_video(
    video_url: str,
    format: str | None,
    format_sort: list[str] | None,
) -> Iterator[DownloadVideoResult]:
    with ExitStack() as stack:
        tmpdir = stack.enter_context(TemporaryDirectory())

        ydl_opts: dict[str, Any] = {
            "paths": {
                "home": tmpdir,
            },
            "cachedir": False,
            "match_filter": match_filter_normal_video_or_live_vod,
        }

        if format is not None:
            ydl_opts["format"] = format

        if format_sort is not None and len(format_sort) > 0:
            ydl_opts["format_sort"] = format_sort

        ydl = stack.enter_context(
            YoutubeDL(
                params=ydl_opts,
            ),
        )

        try:
            returncode = ydl.download(url_list=[video_url])
        except YoutubeDLError as error:
            logger.exception(error)
            raise DownloadVideoError("yt-dlp errored.")

        if returncode != 0:
            raise DownloadVideoError(
                f"Unexpected returncode: {returncode}. Expected: 0."
            )

        tmpdir_path = Path(tmpdir)
        video_file = next(tmpdir_path.iterdir(), None)
        if video_file is None:
            raise DownloadVideoError("No video file.")

        content_type = mimetypes.guess_type(video_file)[0]
        if content_type is None:
            raise DownloadVideoError("Failed to guess content type.")

        if not content_type.startswith("video/") and not content_type.startswith(
            "audio/"
        ):
            raise DownloadVideoError(f"Unsupported content type: {content_type}")

        yield DownloadVideoResult(
            video_file=video_file,
            content_type=content_type,
        )


def lambda_handler(event: dict, context: dict) -> dict:
    bucket = os.environ["AMATERUS_DOWNLOAD_VIDEO_BUCKET"]

    try:
        event_data = AmaterusEnqueueDownloadVideoEvent.model_validate(event)
    except ValidationError as error:
        logger.exception(error)
        return AmaterusDownloadVideoErrorResponse(
            result="error",
            message="Bad Request",
        ).model_dump()

    video_url = event_data.url
    format = event_data.format
    format_sort = event_data.format_sort

    object_key = str(uuid4())

    s3 = boto3.client("s3")
    with ExitStack() as stack:
        try:
            download_result = stack.enter_context(
                download_video(
                    video_url=video_url,
                    format=format,
                    format_sort=format_sort,
                ),
            )
        except DownloadVideoError as error:
            logger.exception(error)
            raise Exception("Failed to download video")

        try:
            s3.upload_file(
                Bucket=bucket,
                Key=object_key,
                Filename=str(download_result.video_file),
                ExtraArgs={
                    "ContentType": download_result.content_type,
                },
            )
        except botocore.exceptions.ClientError as error:
            logger.exception(error)
            raise Exception("Failed to upload video")

    return AmaterusDownloadVideoSuccessResponse(
        result="success",
        message="Ok",
        key=object_key,
    ).model_dump()
