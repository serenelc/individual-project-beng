# Dockerfiles are used to make images

# Run `docker build -t myPythonApp .` in this directory to build THIS image
# example images are ubuntu, python, node etc.. an application running on an operating system or just an OS.

# You will then be able to turn that image into a container `docker run -t myPythonApp`

# # Use the official image as a parent image.
# FROM python:alpine

# # Set the working directory.
# WORKDIR /usr/src/app

# # Copy the rest of your app's source code from your host to your image filesystem.
# COPY src .

# # Run the command inside your image filesystem.
# RUN pip install -r requirements.txt

# # Inform Docker that the container is listening on the specified port at runtime.
# EXPOSE 8080

# # Run the specified command within the container.
# CMD [ "python", "app.py" ]


FROM python:3-alpine
RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev
ADD main.py .
ADD local_helper.py .
ADD local_data_collection.py .
RUN pip3 install psycopg2
CMD [ "python", "./main.py" ]