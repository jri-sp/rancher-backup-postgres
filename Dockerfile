FROM postgres:latest
MAINTAINER Sebastien LANGOUREAUX <linuxworkgroup@hotmail.com>


# Add python and cron to manage backup
RUN apt-get update && \
    apt-get install -y cron python vim python-pip

RUN pip install rancher_metadata

COPY assets/init.py /app/init.py



# CLEAN APT
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


VOLUME ["/backup"]
CMD ["python", "init.py"]
