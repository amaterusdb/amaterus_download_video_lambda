import mimetypes
import os
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator, Literal
from urllib.parse import urlencode
from uuid import uuid4

import boto3
import botocore.exceptions
import ffmpeg
from pydantic import BaseModel, computed_field
from yt_dlp import YoutubeDL
from yt_dlp.utils import YoutubeDLError

logger = getLogger()


class YoutubeVideoOptions(BaseModel):
    video_id: str


class YoutubeVideo(BaseModel):
    site: Literal["youtube"]
    options: YoutubeVideoOptions

    @property
    @computed_field
    def url(self) -> str:
        qs = urlencode(
            {
                "v": self.options.video_id,
            },
        )
        return f"https://www.youtube.com/watch?{qs}"


class AmaterusEnqueueDownloadVideoEvent(BaseModel):
    video: YoutubeVideo


class AmaterusDownloadVideoSuccessResponse(BaseModel):
    result: Literal["success"]
    message: str


class AmaterusDownloadVideoErrorResponse(BaseModel):
    result: Literal["error"]
    message: str


def create_youtube_video_url_from_video_id(video_id: str) -> str:
    qs = urlencode({"v": video_id})
    return f"https://www.youtube.com/watch?{qs}"


@dataclass
class DownloadVideoResult:
    video_file: Path
    content_type: str
    size: str
    duration: str
    width: int | None
    height: int | None
    frame_count: str | None
    video_codec_name: str | None
    video_codec_tag_string: str | None
    audio_codec_name: str | None
    audio_codec_tag_string: str | None


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
            returncode = ydl.download([video_url])
        except YoutubeDLError:
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

        size = str(video_file.stat().st_size)

        try:
            probe = ffmpeg.probe(str(video_file))
        except ffmpeg.Error:
            raise DownloadVideoError("ffmpeg errored.")

        format = probe["format"]

        video_stream = next(
            (stream for stream in probe["streams"] if stream["codec_type"] == "video"),
            None,
        )
        audio_stream = next(
            (stream for stream in probe["streams"] if stream["codec_type"] == "audio"),
            None,
        )

        duration = str(format["duration"])

        if video_stream is not None:
            width = int(video_stream["width"])
            height = int(video_stream["height"])
            frame_count = str(video_stream["nb_frames"])
            video_codec_name = video_stream["codec_name"]
            video_codec_tag_string = video_stream["codec_tag_string"]
        else:
            width = None
            height = None
            frame_count = None
            video_codec_name = None
            video_codec_tag_string = None

        if audio_stream is not None:
            audio_codec_name = audio_stream["codec_name"]
            audio_codec_tag_string = audio_stream["codec_tag_string"]
        else:
            audio_codec_name = None
            audio_codec_tag_string = None

        yield DownloadVideoResult(
            video_file=video_file,
            content_type=content_type,
            size=size,
            duration=duration,
            width=width,
            height=height,
            frame_count=frame_count,
            video_codec_name=video_codec_name,
            video_codec_tag_string=video_codec_tag_string,
            audio_codec_name=audio_codec_name,
            audio_codec_tag_string=audio_codec_tag_string,
        )


def lambda_handler(event: dict, context: dict) -> None:
    table_name = os.environ["AMATERUS_DOWNLOAD_VIDEO_TABLE_NAME"]
    bucket = os.environ["AMATERUS_DOWNLOAD_VIDEO_BUCKET"]

    dynamodb = boto3.client("dynamodb")

    # SQS message event
    records = event["Records"]
    for record in records:
        event_name = record["eventName"]
        if event_name != "aws:sqs":
            continue

        video_id = record["body"]

        try:
            downloading_video_item_result = dynamodb.update_item(
                TableName=table_name,
                Key={"VideoId": {"S", video_id}},
                AttributeUpdates={
                    "Status": {
                        "Action": "PUT",
                        "Value": {"S": "downloading"},
                    },
                },
                ReturnValues="ALL_NEW",
            )
        except botocore.exceptions.ClientError:
            raise Exception("Failed to update status to 'downloading'.")

        downloading_video_item = downloading_video_item_result["Attributes"]
        source = downloading_video_item["Source"]["S"]

        video_url: str | None = None
        if source == "youtube":
            youtube_video_id = downloading_video_item["YoutubeVideoId"]["S"]
            video_url = create_youtube_video_url_from_video_id(
                video_id=youtube_video_id
            )
        else:
            raise Exception(f"Unsupported source: {source}")

        object_key = str(uuid4())

        s3 = boto3.client("s3")
        try:
            with download_video(video_url=video_url) as download_result:
                try:
                    s3.upload_file(
                        Bucket=bucket,
                        Key=object_key,
                        Filename=str(download_result.video_file),
                        ExtraArgs={
                            "ContentType": download_result.content_type,
                        },
                    )
                except botocore.exceptions.ClientError:
                    raise Exception("Failed to update status to 'downloaded'.")
        except DownloadVideoError:
            raise Exception("Failed to download video.")

        try:
            dynamodb.update_item(
                TableName=table_name,
                Key={"VideoId": {"S", video_id}},
                AttributeUpdates={
                    "Status": {
                        "Action": "PUT",
                        "Value": {"S": "downloaded"},
                    },
                    "ObjectKey": {
                        "Action": "PUT",
                        "Value": {"S": object_key},
                    },
                    "ContentType": {
                        "Action": "PUT",
                        "Value": {"S": download_result.content_type},
                    },
                    "Size": {
                        "Action": "PUT",
                        "Value": {"S": download_result.size},
                    },
                    "Duration": {
                        "Action": "PUT",
                        "Value": {"S": download_result.duration},
                    },
                    "Width": {
                        "Action": "PUT",
                        "Value": {"N": download_result.width},
                    },
                    "Height": {
                        "Action": "PUT",
                        "Value": {"N": download_result.height},
                    },
                    "FrameCount": {
                        "Action": "PUT",
                        "Value": {"S": download_result.frame_count},
                    },
                    "VideoCodecName": {
                        "Action": "PUT",
                        "Value": {"S": download_result.video_codec_name},
                    },
                    "VideoCodecTagString": {
                        "Action": "PUT",
                        "Value": {"S": download_result.video_codec_tag_string},
                    },
                    "AudioCodecName": {
                        "Action": "PUT",
                        "Value": {"S": download_result.audio_codec_name},
                    },
                    "AudioCodecTagString": {
                        "Action": "PUT",
                        "Value": {"S": download_result.audio_codec_tag_string},
                    },
                },
            )
        except botocore.exceptions.ClientError:
            raise Exception("Failed to update status to 'downloaded'.")
