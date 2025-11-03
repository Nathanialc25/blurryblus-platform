
#9/8
#Good to commit

# Use a slim Python image
FROM python:3.11-slim-bullseye

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first (for better Docker layer caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Tell Flask which app to run
ENV FLASK_APP=app.py

# Expose the port Flask runs on inside the container
EXPOSE 5000

# The command to run the application
#CMD ["flask", "run", "--host=0.0.0.0"]

# 10/16 new method to just run python directly instead of flask run like above. this should help with cloud run, and still allow for local dev
CMD ["python", "app.py"]