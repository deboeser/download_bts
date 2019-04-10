FROM python:3
RUN mkdir -p /usr/src/download
WORKDIR /usr/src/download
RUN pip install tqdm pandas requests
COPY . .
CMD tail -f /dev/null
# CMD ["python", "download.py"]
