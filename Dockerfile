FROM kernai/refinery-parent-images:v1.7.0-common

WORKDIR /program

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt
RUN python -m spacy download en_core_web_sm
RUN python -m spacy download de_core_news_sm

COPY / .

CMD [ "/usr/local/bin/uvicorn", "--host", "0.0.0.0", "--port", "80", "app:app" ]