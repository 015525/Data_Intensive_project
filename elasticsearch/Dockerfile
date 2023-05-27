FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the Python requirements file
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python file to the working directory
COPY main.py .

# Set the command to run the Python file
CMD ["python", "main.py"]

