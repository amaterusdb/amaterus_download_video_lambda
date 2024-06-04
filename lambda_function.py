import mimetypes
import os
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator, Literal
from uuid import uuid4

import boto3
import botocore.exceptions
from pydantic import BaseModel, Field, ValidationError
from yt_dlp import YoutubeDL
from yt_dlp.utils import YoutubeDLError

logger = getLogger()


class AmaterusEnqueueDownloadVideoEvent(BaseModel):
    url: str = Field(serialization_alias="Url")


class AmaterusDownloadVideoSuccessResponse(BaseModel):
    result: Literal["success"] = Field(serialization_alias="Result")
    message: str = Field(serialization_alias="Message")
    url: str = Field(serialization_alias="Url")


class AmaterusDownloadVideoErrorResponse(BaseModel):
    result: Literal["error"] = Field(serialization_alias="Result")
    message: str = Field(serialization_alias="Message")


@dataclass
class DownloadVideoResult:
    video_file: Path
    content_type: str


class DownloadVideoError(Exception):
    pass


@contextmanager
def download_video(video_url: str) -> Iterator[DownloadVideoResult]:
    with ExitStack() as stack:
        tmpdir = stack.enter_context(TemporaryDirectory())

        ydl = stack.enter_context(
            YoutubeDL(
                params={
                    "paths": {
                        "home": tmpdir,
                    },
                },
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

    object_key = str(uuid4())

    s3 = boto3.client("s3")
    with ExitStack() as stack:
        try:
            download_result = stack.enter_context(download_video(video_url=video_url))
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

    downloaded_video_url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": bucket,
            "Key": object_key,
        },
        ExpiresIn=3600,
    )

    return AmaterusDownloadVideoSuccessResponse(
        result="success",
        message="Ok",
        url=downloaded_video_url,
    ).model_dump()
