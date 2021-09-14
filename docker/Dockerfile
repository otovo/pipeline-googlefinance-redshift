FROM python:3.9-slim
ADD VERSION .

WORKDIR /usr/src/app

# Install dependencies
COPY requirements.txt ./
RUN \
  pip3 install --upgrade setuptools && \
  pip3 install --no-cache-dir -r requirements.txt

# Entrypoint
COPY app/app.py ./app.py
CMD [ "python", "app.py" ]
