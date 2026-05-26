ARG PARENT_IMAGE=kernai/refinery-parent-images:v2.5.0-common

FROM ${PARENT_IMAGE} AS builder

ENV VENV_PATH=/opt/venv
ENV PATH="${VENV_PATH}/bin:${PATH}"

WORKDIR /program

USER root

RUN if [ ! -d "${VENV_PATH}" ]; then python -m venv "${VENV_PATH}"; fi

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

USER root

COPY --from=builder --chown=65532:65532 ${VENV_PATH} ${VENV_PATH}
COPY --from=builder --chown=65532:65532 /program /program

USER 65532:65532

CMD ["/opt/venv/bin/uvicorn", "--host", "0.0.0.0", "--port", "80", "app:app"]
