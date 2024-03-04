# Use an official Python runtime based on Debian 10 ("Buster") as a parent image
FROM python:3.8-slim-buster

# Set environment variables
ENV DEBIAN_FRONTEND=non-interactive

# Update packages and install curl for HTTP requests
RUN apt-get update -y
RUN pip3 install --upgrade pip
RUN apt-get install -y curl
RUN rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY . /app

# Set the working directory in the container to /app
WORKDIR /app



# Install any needed packages specified in requirements.txt and environment.yml
# Pip packages
RUN pip install -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Run your_flask_app.py when the container launches
CMD ["python", "app.py"]