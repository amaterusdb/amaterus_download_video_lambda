# syntax=docker/dockerfile:1.7
FROM python:3.12 AS poetry-export-stage

ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHONUNBUFFERED=1

ARG CONTAINER_UID=999
ARG CONTAINER_GID=999
RUN <<EOF
    set -eu

    groupadd --non-unique --gid "${CONTAINER_GID}" user
    useradd --non-unique --uid "${CONTAINER_UID}" --gid "${CONTAINER_GID}" --create-home user
EOF

RUN <<EOF
    set -eu

    mkdir -p /work
    chown -R "${CONTAINER_UID}:${CONTAINER_GID}" /work
EOF

USER user
WORKDIR /work
ARG PATH="/home/user/.local/bin:${PATH}"

RUN <<EOF
    set -eu

    mkdir -p /home/user/.cache/pip
EOF

ARG PIPX_VERSION=1.5.0
RUN --mount=type=cache,uid="${CONTAINER_UID}",gid="${CONTAINER_GID}",target=/home/user/.cache/pip <<EOF
    set -eu

    pip install "pipx==${PIPX_VERSION}"
    mkdir -p /home/user/.cache/pipx
EOF

ARG POETRY_VERSION=1.8.3
RUN --mount=type=cache,uid="${CONTAINER_UID}",gid="${CONTAINER_GID}",target=/home/user/.cache/pipx <<EOF
    set -eu

    pipx install "poetry==${POETRY_VERSION}"
    mkdir -p /home/user/.cache/pypoetry/{cache,artifacts}
EOF

RUN --mount=type=cache,uid="${CONTAINER_UID}",gid="${CONTAINER_GID}",target=/home/user/.cache/pypoetry/cache \
    --mount=type=cache,uid="${CONTAINER_UID}",gid="${CONTAINER_GID}",target=/home/user/.cache/pypoetry/artifacts <<EOF
    set -eu

    poetry self add poetry-plugin-export
EOF

COPY ./pyproject.toml ./poetry.lock /work/
RUN poetry export -o requirements.txt

FROM ubuntu:22.04 AS ffmpeg-stage

ARG DEBIAN_FRONTEND=noninteractive

RUN <<EOF
    set -eu

    apt-get update

    apt-get install -y \
        wget \
        xz-utils

    apt-get clean
    rm -rf /var/lib/apt/lists/*
EOF

WORKDIR /work
ARG FFMPEG_URL="https://github.com/amaterusdb/amaterus_ffmpeg_mirror_releases/releases/download/v0.1.0.20240603/ffmpeg-git-20240524-arm64-static.tar.xz"
ARG FFMPEG_HASH_SHA256="baa6a79a305e8762e9c22e1208fb8717c58d97fa89235ccf9fb18aa0f6d192a1"
RUN <<EOF
    set -eu

    wget -O ffmpeg.tar.xz "${FFMPEG_URL}"
    echo -n "${FFMPEG_HASH_SHA256}  ffmpeg.tar.xz" | sha256sum --check -

    mkdir ./ffmpeg
    tar --strip-components 1 -C ./ffmpeg/ -xf ffmpeg.tar.xz

    mv ./ffmpeg/ /opt/ffmpeg
EOF

FROM public.ecr.aws/lambda/python:3.12 AS runtime-stage

ENV PATH=/opt/ffmpeg:${PATH}
COPY --from=ffmpeg-stage /opt/ffmpeg /opt/ffmpeg

COPY --from=poetry-export-stage /work/requirements.txt "${LAMBDA_TASK_ROOT}"
RUN pip install -r "${LAMBDA_TASK_ROOT}/requirements.txt"

COPY ./lambda_function.py "${LAMBDA_TASK_ROOT}"

CMD [ "lambda_function.lambda_handler" ]
