FROM python:3.11

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install custom python operator
RUN git clone https://github.com/fbardos/python_docker_operator.git operator \
    && pip install operator/

# Copy the rest of the code (to avoid re-installing dependencies on code changes)
COPY . .
