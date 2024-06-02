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

FROM public.ecr.aws/lambda/python:3.12 AS runtime-stage

COPY --from=poetry-export-stage /work/requirements.txt "${LAMBDA_TASK_ROOT}"
RUN pip install -r "${LAMBDA_TASK_ROOT}/requirements.txt"

COPY ./lambda_function.py "${LAMBDA_TASK_ROOT}"

CMD [ "lambda_function.lambda_handler" ]
