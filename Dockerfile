ARG PARENT_IMAGE=registry.dev.kern.ai/code-kern-ai/refinery-parent-images:hardened-images-common
ARG DHI_PYTHON_BUILD=dhi.io/python:3.11-debian12-dev

FROM ${PARENT_IMAGE} AS venv-source

FROM ${DHI_PYTHON_BUILD} AS builder

ENV VENV_PATH=/opt/venv
ENV PATH="${VENV_PATH}/bin:${PATH}"

WORKDIR /program

COPY --from=venv-source ${VENV_PATH} ${VENV_PATH}

RUN apt-get update && \
    apt-get install --no-install-recommends -y curl libc6-dev zlib1g gcc && \
    rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt
RUN python -m spacy download en_core_web_sm
RUN python -m spacy download de_core_news_sm

COPY . .

FROM ${PARENT_IMAGE}

ENV VENV_PATH=/opt/venv
ENV PATH="${VENV_PATH}/bin:${PATH}"

WORKDIR /program

COPY --from=builder --chown=65532:65532 ${VENV_PATH} ${VENV_PATH}
COPY --from=builder --chown=65532:65532 /program /program

USER nonroot

CMD ["/opt/venv/bin/uvicorn", "--host", "0.0.0.0", "--port", "80", "app:app"]
