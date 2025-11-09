FROM python:3.11-slim

# Work directory inside the container
WORKDIR /app

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker code
COPY worker.py /app/worker.py

# Expose RPyC port
EXPOSE 18861

# Run the RPyC worker service
CMD ["python", "worker.py"]
