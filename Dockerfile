FROM python:3.4
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install pyyaml
RUN pip install requests
COPY . /usr/src/app
CMD [ "python", "-u", "./__init__.py", "--force" ]
