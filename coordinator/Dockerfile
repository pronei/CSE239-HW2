FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy coordinator code
COPY coordinator.py /app/coordinator.py

# No ports needed; coordinator is just an RPC client
CMD ["python", "coordinator.py"]
